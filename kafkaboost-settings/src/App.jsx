// App.jsx — show full error list when save is blocked (range/validation),
// priorities in [0..MaxPriority], empty priority -> DefaultPriority,
// mutually-exclusive messages, Logout, History modal, labeled fields, + USER_ID viewer button.
import React, { useState, useEffect } from 'react';
import './App.css';
import logo from './assets/kafkaboost-logo.png';
import { Amplify } from 'aws-amplify';
import awsconfig from './aws-exports';
import { getCurrentUser, signOut } from 'aws-amplify/auth';
import { uploadData, list, getUrl } from 'aws-amplify/storage';
import { withAuthenticator } from '@aws-amplify/ui-react';
import '@aws-amplify/ui-react/styles.css';

Amplify.configure(awsconfig);

function App() {
  const [stage, setStage] = useState('defineMax');
  const [topics, setTopics] = useState([]);
  const [valRules, setValRules] = useState([]);
  const [priorityBoost, setPriorityBoost] = useState([]);
  const [partitionValue, setPartitionValue] = useState('');
  const [defaultPriority, setDefaultPriority] = useState('');
  const [maxPriority, setMaxPriority] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [errorMessage, setErrorMessage] = useState('');
  const [editingSettings, setEditingSettings] = useState(false);
  const [history, setHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [isHistoryOpen, setIsHistoryOpen] = useState(false);
  const [userId, setUserId] = useState('');
  const [userEmail, setUserEmail] = useState('');
  const [showUserId, setShowUserId] = useState(false);
  const [copiedUserId, setCopiedUserId] = useState(false);
  const [userLoadError, setUserLoadError] = useState('');

  useEffect(() => {
    (async () => {
      try {
        const user = await getCurrentUser();
        setUserId(user.userId || '');
        setUserEmail(user?.signInDetails?.loginId || '');
      } catch (e) {
        console.warn('Could not load user for USER_ID panel', e);
        setUserLoadError('לא ניתן לטעון פרטי משתמש (USER_ID). ודא/י שהינך מחובר/ת.');
      }
    })();

    fetchSettingsVersions();
    loadLatestSettings();
  }, []);

  // --------------------
  // Messaging (mutually exclusive)
  // --------------------
  const showError = (msg) => {
    setErrorMessage(msg);
    setSuccessMessage('');
  };
  const showSuccess = (msg) => {
    setSuccessMessage(msg);
    setErrorMessage('');
  };

  // --------------------
  // Logout
  // --------------------
  const handleLogout = async () => {
    try {
      await signOut();
      setSuccessMessage('');
      setErrorMessage('');
    } catch (e) {
      console.error('Logout failed:', e);
      showError('❌ Failed to logout. Please try again.');
    }
  };

  // --------------------
  // Required fields (priority is optional)
  // --------------------
  const getMissingRequiredErrors = () => {
    const errs = [];

    if (defaultPriority === '' || maxPriority === '') {
      errs.push('Default Priority and Max Priority are required.');
    }

     if (defaultPriority > maxPriority) {
      errs.push('Default Priority has to be in range 0 - Max Priority.');
    }

    topics.forEach((t, i) => {
      const touched = (t.name?.trim() !== '') || (t.priority?.trim() !== '');
      if (touched && (t.name?.trim() === '')) {
        errs.push(`Topics row ${i + 1}: Topic Name is required when row is filled.`);
      }
    });

    valRules.forEach((r, i) => {
      const touched = (r.role_name?.trim() !== '') || (r.val?.trim() !== '') || (r.priority?.trim() !== '');
      if (touched && (r.role_name?.trim() === '' || r.val?.trim() === '')) {
        errs.push(`Value-Based Rules row ${i + 1}: Role Name and Value are required when row is filled.`);
      }
    });

    priorityBoost.forEach((b, i) => {
      const touched = (b.topic_name?.trim() !== '') || (b.priority_boost_min_value?.trim() !== '') || (b.number_of_partitions?.trim() !== '');
      if (touched && (b.topic_name?.trim() === '' || b.priority_boost_min_value?.trim() === '')) {
        errs.push(`Priority Boost row ${i + 1}: Topic and Boost Min Value are required when row is filled.`);
      }
    });

    return errs;
  };

  const hasMissingRequiredFields = () => getMissingRequiredErrors().length > 0;
  const canSave = !hasMissingRequiredFields();

  // --------------------
  // Normalize & Validation
  // --------------------
  const normalizePriority = (val, defaultP) => {
    return (val === '' || val === null || val === undefined)
      ? defaultP
      : parseInt(val);
  };

  // Build a full list of validation errors. If list is empty => valid.
  const collectValidationErrors = () => {
    const errs = [];
    const defP = parseInt(defaultPriority);
    const maxP = parseInt(maxPriority);

    if (defaultPriority === '' || maxPriority === '') {
      errs.push('Default Priority and Max Priority are required.');
      return errs;
    }
    if (isNaN(defP) || isNaN(maxP)) {
      errs.push('Default/Max Priority must be numeric.');
      return errs;
    }
    if (maxP < 0) errs.push('Max Priority must be ≥ 0.');
    if (defP < 0 || defP > maxP) errs.push(`Default Priority must be within [0, ${maxP}].`);

    const topicsToSave = topics.filter(t => (t.name || '').trim() !== '');
    topicsToSave.forEach((t, i) => {
      const n = normalizePriority(t.priority, defP);
      if (isNaN(n) || n < 0 || n > maxP) {
        errs.push(`Topics row ${i + 1}: priority ${t.priority || `(empty→${defP})`} is out of range [0, ${maxP}].`);
      }
    });

    const rulesToSave = valRules.filter(v => (v.role_name || '').trim() !== '' && (v.val ?? '') !== '');
    rulesToSave.forEach((r, i) => {
      const n = normalizePriority(r.priority, defP);
      if (isNaN(n) || n < 0 || n > maxP) {
        errs.push(`Value-Based Rules row ${i + 1}: priority ${r.priority || `(empty→${defP})`} is out of range [0, ${maxP}].`);
      }
    });

    // Priority Boost: no numeric range check here for priority, only required done above
    return errs;
  };

  // --------------------
  // Save
  // --------------------
  const handleSubmit = async () => {
    setSuccessMessage('');

    // 1) Detailed required-field errors first
    const missing = getMissingRequiredErrors();
    if (missing.length > 0) {
      showError(`❌ Save blocked due to missing required fields:\n- ${missing.join('\n- ')}`);
      return;
    }

    // 2) Range / numeric validations
    const allErrors = collectValidationErrors();
    if (allErrors.length > 0) {
      showError(`❌ Save blocked due to validation errors:\n- ${allErrors.join('\n- ')}`);
      return;
    }

    try {
      const user = await getCurrentUser();
      const userId = user.userId;
      const defP = parseInt(defaultPriority);
      const maxP = parseInt(maxPriority);

      const Topics_priority = topics
        .filter(t => (t.name || '').trim() !== '')
        .map(t => ({
          topic: t.name.trim(),
          priority: normalizePriority(t.priority, defP)
        }));

      const Rule_Base_priority = valRules
        .filter(v => (v.role_name || '').trim() !== '' && (v.val ?? '') !== '')
        .map(v => ({
          role_name: v.role_name.trim(),
          value: v.val,
          priority: normalizePriority(v.priority, defP)
        }));

      const Priority_boost = priorityBoost.map(b => ({
        topic_name: (b.topic_name || '').trim(),
        priority_boost_min_value: parseInt(b.priority_boost_min_value || '0'),
        number_of_partitions: parseInt(b.number_of_partitions || '0')
      }));

      const settings = {
        user_id: userId,
        max_priority: maxP,
        default_priority: defP,
        Topics_priority,
        Rule_Base_priority,
        Priority_boost
      };

      const key = `users/${userId}/settings-${new Date().toISOString()}.json`;

      await uploadData({
        key,
        data: JSON.stringify(settings, null, 2),
        options: { contentType: 'application/json', accessLevel: 'private' }
      }).result;

      showSuccess('✅ Settings saved successfully!');
      fetchSettingsVersions();
    } catch (err) {
      console.error('Save failed:', err);
      const msg = err?.message ? `❌ Failed to save: ${err.message}` : '❌ Failed to save settings to cloud.';
      showError(msg);
    }
  };

  // --------------------
  // Cloud (history)
  // --------------------
  const fetchSettingsVersions = async () => {
    setLoadingHistory(true);
    try {
      const user = await getCurrentUser();
      const userId = user.userId;
      const result = await list({ prefix: `users/${userId}/`, options: { accessLevel: 'private' } });
      const files = result.items
        .filter(file => file.key.endsWith('.json'))
        .sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified));
      setHistory(files);
    } catch (e) {
      console.warn('Failed to load history:', e);
    } finally {
      setLoadingHistory(false);
    }
  };

  const loadLatestSettings = async () => {
    try {
      const user = await getCurrentUser();
      const userId = user.userId;
      const result = await list({ prefix: `users/${userId}/`, options: { accessLevel: 'private' } });
      const files = result.items
        .filter(i => i.key.endsWith('.json'))
        .sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified));
      if (!files.length) return;

      const url = await getUrl({ key: files[0].key, options: { accessLevel: 'private', expiresIn: 300 } });
      const response = await fetch(url.url);
      const loaded = await response.json();

      setTopics((loaded.Topics_priority || []).map(t => ({ name: t.topic, priority: t.priority.toString() })));
      setValRules((loaded.Rule_Base_priority || []).map(r => ({ role_name: r.role_name || '', val: r.value, priority: r.priority.toString() })));
      setPriorityBoost((loaded.Priority_boost || []).map(p => ({
        topic_name: p.topic_name,
        priority_boost_min_value: p.priority_boost_min_value.toString(),
        number_of_partitions: p.number_of_partitions?.toString() || ''
      })));
      setDefaultPriority(loaded.default_priority?.toString() || '');
      setMaxPriority(loaded.max_priority?.toString() || '');
      setStage('settings');
    } catch (err) {
      console.warn('No settings found.', err);
    }
  };

  const loadVersion = async (key) => {
    try {
      const url = await getUrl({ key, options: { accessLevel: 'private', expiresIn: 300 } });
      const response = await fetch(url.url);
      const loaded = await response.json();

      setTopics((loaded.Topics_priority || []).map(t => ({ name: t.topic, priority: t.priority.toString() })));
      setValRules((loaded.Rule_Base_priority || []).map(r => ({ role_name: r.role_name || '', val: r.value, priority: r.priority.toString() })));
      setPriorityBoost((loaded.Priority_boost || []).map(p => ({
        topic_name: p.topic_name,
        priority_boost_min_value: p.priority_boost_min_value.toString(),
        number_of_partitions: p.number_of_partitions?.toString() || ''
      })));
      setDefaultPriority(loaded.default_priority?.toString() || '');
      setMaxPriority(loaded.max_priority?.toString() || '');
    } catch (err) {
      console.error('Failed to load version', err);
      showError('❌ Failed to load the selected version.');
    }
  };

  const applyPartitionsToAll = () => {
    if (!partitionValue || isNaN(parseInt(partitionValue))) {
      showError('❌ Please enter a valid number of partitions.');
      return;
    }
    setPriorityBoost(priorityBoost.map(p => ({ ...p, number_of_partitions: partitionValue })));
  };

  // --------------------
  // Render
  // --------------------
  return (
    <div className="page-container">
      <img src={logo} alt="KafkaBoost Logo" className="logo" />

      <div className="form-container">
        {/* Header actions */}
        <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end', marginBottom: 12, flexWrap: 'wrap' }}>
          <button className="button-style" onClick={() => setIsHistoryOpen(true)}>📜 Open History</button>

          {/* USER_ID viewer toggle */}
          <button className="button-style" onClick={() => setShowUserId(s => !s)}>
            {showUserId ? '🙈 Hide USER_ID' : '👤 Show USER_ID'}
          </button>

          <button className="button-style" onClick={handleLogout}>🚪 Logout</button>
        </div>

        {/* USER_ID panel */}
        {showUserId && (
          <div className="user-panel">
            {userLoadError ? (
              <div className="error-message">{userLoadError}</div>
            ) : (
              <>
                <div className="user-row"><b>Email:</b> {userEmail || '(unknown)'} </div>
                <div className="user-row" style={{ wordBreak: 'break-all' }}><b>USER_ID:</b> {userId}</div>
                <div style={{ marginTop: 8 }}>
                  <button
                    className="button-style"
                    onClick={async () => {
                      try {
                        await navigator.clipboard.writeText(userId);
                        setCopiedUserId(true);
                        setTimeout(() => setCopiedUserId(false), 1200);
                      } catch (e) {
                        console.error('Copy failed', e);
                        showError('❌ Failed to copy USER_ID.');
                      }
                    }}
                  >
                    {copiedUserId ? '✔ Copied' : 'Copy USER_ID'}
                  </button>
                </div>
              </>
            )}
          </div>
        )}

        {/* Messages (success OR error). Error supports multi-line display */}
        {errorMessage && (
          <div className="error-message" style={{ whiteSpace: 'pre-line' }}>
            {errorMessage}
          </div>
        )}
        {!errorMessage && successMessage && (
          <div className="success-message">
            {successMessage}
          </div>
        )}

        {/* History Modal */}
        {isHistoryOpen && (
          <div className="modal-backdrop" onClick={() => setIsHistoryOpen(false)}>
            <div className="modal" onClick={e => e.stopPropagation()}>
              <div className="modal-header">
                <h3 style={{ margin: 0 }}>Settings History</h3>
                <button className="close-button" onClick={() => setIsHistoryOpen(false)}>✖</button>
              </div>
              <div className="modal-body">
                {loadingHistory ? (
                  <p>Loading...</p>
                ) : history.length === 0 ? (
                  <p>No previous settings found.</p>
                ) : (
                  <ul className="history-list">
                    {history.map((h, i) => (
                      <li key={i}>
                        <button
                          className="linklike"
                          onClick={() => {
                            loadVersion(h.key);
                            setIsHistoryOpen(false);
                          }}
                        >
                          {new Date(h.lastModified).toLocaleString()}
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Step 1: define defaults */}
        {stage === 'defineMax' ? (
          <>
            <h1>Settings</h1>

            <div className="input-block">
              <label>DEFAULT PRIORITY</label>
              <input
                type="number"
                min="0"
                placeholder="Enter default priority (e.g., 8)"
                value={defaultPriority}
                onChange={e => { setDefaultPriority(e.target.value.replace(/\D/g, '')); setSuccessMessage(''); }}
                className="input-style"
              />
            </div>

            <div className="input-block">
              <label>MAX PRIORITY</label>
              <input
                type="number"
                min="0"
                placeholder="Enter max priority (e.g., 64)"
                value={maxPriority}
                onChange={e => { setMaxPriority(e.target.value.replace(/\D/g, '')); setSuccessMessage(''); }}
                className="input-style"
              />
            </div>

            <button
              onClick={() => {
                setSuccessMessage('');
                const missing = getMissingRequiredErrors();
                if (missing.length > 0) {
                  showError(`❌ Save blocked due to missing required fields:\n- ${missing.join('\n- ')}`);
                  return;
                }
                const allErrors = collectValidationErrors();
                if (allErrors.length > 0) {
                  showError(`❌ Save blocked due to validation errors:\n- ${allErrors.join('\n- ')}`);
                  return;
                }
                setErrorMessage('');
                setStage('settings');
              }}
              className="button-style"
              disabled={!canSave}
            >
              Save Settings
            </button>
          </>
        ) : (
          <>
            <h2>Settings</h2>

            {/* Edit / View Defaults */}
            <div style={{ marginBottom: '20px' }}>
              {editingSettings ? (
                <>
                  <div className="input-block">
                    <label>DEFAULT PRIORITY</label>
                    <input
                      type="number"
                      min="0"
                      placeholder="Enter default priority"
                      value={defaultPriority}
                      onChange={e => { setDefaultPriority(e.target.value.replace(/\D/g, '')); setSuccessMessage(''); }}
                      className="input-style"
                    />
                  </div>

                  <div className="input-block">
                    <label>MAX PRIORITY</label>
                    <input
                      type="number"
                      min="0"
                      placeholder="Enter max priority"
                      value={maxPriority}
                      onChange={e => { setMaxPriority(e.target.value.replace(/\D/g, '')); setSuccessMessage(''); }}
                      className="input-style"
                    />
                  </div>

                  <button className="save-button" onClick={() => setEditingSettings(false)} disabled={!canSave}>
                    Save Settings
                  </button>
                </>
              ) : (
                <>
                  <p><b>Default Priority:</b> {defaultPriority}</p>
                  <p><b>Max Priority:</b> {maxPriority}</p>
                  <button className="edit-button" onClick={() => setEditingSettings(true)}>Edit Settings</button>
                </>
              )}
            </div>

            {/* Topics Table */}
            <div className="table-header">
              <h3>Topics <button onClick={() => setTopics([...topics, { name: '', priority: '' }])} className="plus-button">➕</button></h3>
            </div>
            <table className="styled-table">
              <thead><tr><th>Topic Name</th><th>Priority</th><th>Actions</th></tr></thead>
              <tbody>
                {topics.map((topic, index) => (
                  <tr key={index}>
                    <td>
                      <input
                        value={topic.name}
                        onChange={(e) => {
                          const updated = [...topics]; updated[index].name = e.target.value; setTopics(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <input
                        value={topic.priority}
                        onChange={(e) => {
                          const v = e.target.value.replace(/[^0-9]/g, '');
                          const updated = [...topics]; updated[index].priority = v; setTopics(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <button className="delete-button" onClick={() => { setTopics(topics.filter((_, i) => i !== index)); setSuccessMessage(''); }}>Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Value-Based Rules */}
            <div className="table-header">
              <h3>Value-Based Rules <button onClick={() => setValRules([...valRules, { role_name: '', val: '', priority: '' }])} className="plus-button">➕</button></h3>
            </div>
            <table className="styled-table">
              <thead><tr><th>Role Name</th><th>Value</th><th>Priority</th><th>Actions</th></tr></thead>
              <tbody>
                {valRules.map((val, index) => (
                  <tr key={index}>
                    <td>
                      <input
                        value={val.role_name}
                        onChange={(e) => {
                          const updated = [...valRules]; updated[index].role_name = e.target.value; setValRules(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <input
                        value={val.val}
                        onChange={(e) => {
                          const updated = [...valRules]; updated[index].val = e.target.value; setValRules(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <input
                        value={val.priority}
                        onChange={(e) => {
                          const v = e.target.value.replace(/[^0-9]/g, '');
                          const updated = [...valRules]; updated[index].priority = v; setValRules(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <button className="delete-button" onClick={() => { setValRules(valRules.filter((_, i) => i !== index)); setSuccessMessage(''); }}>Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Priority Boost */}
            <div className="table-header">
              <h3>Priority Boost <button onClick={() => setPriorityBoost([...priorityBoost, { topic_name: '', priority_boost_min_value: '', number_of_partitions: '' }])} className="plus-button">➕</button></h3>
            </div>
            <div style={{ display: 'flex', gap: '10px', marginBottom: '10px' }}>
              <input
                placeholder="Set All Partitions"
                value={partitionValue}
                onChange={(e) => { setPartitionValue(e.target.value); setSuccessMessage(''); }}
                className="input-style"
              />
              <button onClick={applyPartitionsToAll} className="button-style">Set All</button>
            </div>
            <table className="styled-table">
              <thead><tr><th>Topic</th><th>Boost Min Value</th><th>Partitions</th><th>Actions</th></tr></thead>
              <tbody>
                {priorityBoost.map((boost, index) => (
                  <tr key={index}>
                    <td>
                      <input
                        value={boost.topic_name}
                        onChange={(e) => {
                          const updated = [...priorityBoost]; updated[index].topic_name = e.target.value; setPriorityBoost(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <input
                        value={boost.priority_boost_min_value}
                        onChange={(e) => {
                          const updated = [...priorityBoost]; updated[index].priority_boost_min_value = e.target.value; setPriorityBoost(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <input
                        value={boost.number_of_partitions || ''}
                        onChange={(e) => {
                          const updated = [...priorityBoost]; updated[index].number_of_partitions = e.target.value; setPriorityBoost(updated); setSuccessMessage('');
                        }}
                        className="input-style"
                      />
                    </td>
                    <td>
                      <button className="delete-button" onClick={() => { setPriorityBoost(priorityBoost.filter((_, i) => i !== index)); setSuccessMessage(''); }}>Delete</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>

            {/* allow clicking even if required missing so user gets full error list */}
            <button className="button-style" onClick={handleSubmit}>💾 Save All Settings</button>
          </>
        )}
      </div>
    </div>
  );
}

export default withAuthenticator(App);
