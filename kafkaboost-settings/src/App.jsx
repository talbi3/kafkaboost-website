import React, { useState } from 'react';
import './App.css';
import logo from './assets/kafkaboost-logo.png';
import rocketIcon from './assets/rocket-icon.png';


function App() {
  const [stage, setStage] = useState('defineMax');
  const [topics, setTopics] = useState([]);
  const [valRules, setValRules] = useState([]);
  const [defaultPriority, setDefaultPriority] = useState('');
  const [maxPriority, setMaxPriority] = useState('');
  const [editingSettings, setEditingSettings] = useState(false);
  const [editingTopicIndex, setEditingTopicIndex] = useState(null);
  const [editingValRuleIndex, setEditingValRuleIndex] = useState(null);
  const [errorMessage, setErrorMessage] = useState('');
  const [successMessage, setSuccessMessage] = useState('');
  const [successFullScreen, setSuccessFullScreen] = useState(false);
  const [topicErrors, setTopicErrors] = useState({});
  const [valRuleErrors, setValRuleErrors] = useState({});

  const handleMaxPrioritySubmit = () => {
    if (!defaultPriority || !maxPriority || isNaN(parseInt(maxPriority)) || parseInt(maxPriority) < 1 || isNaN(parseInt(defaultPriority))) {
      setErrorMessage('Please enter valid Default and Max Priority values.');
      return;
    }
    if (parseInt(defaultPriority) > parseInt(maxPriority)) {
      setErrorMessage('Default Priority must be less than or equal to Max Priority.');
      return;
    }
    setErrorMessage('');
    setStage('settings');
  };

  const validatePriority = (priority) => {
    const num = parseInt(priority);
    return !isNaN(num) && num >= 1 && num <= parseInt(maxPriority);
  };

  const addTopic = () => {
    setTopics([...topics, { name: '', priority: '' }]);
    setEditingTopicIndex(topics.length);
  };

  const addValRule = () => {
    setValRules([...valRules, { val: '', priority: '' }]);
    setEditingValRuleIndex(valRules.length);
  };

  const updateTopic = (index, field, value) => {
    const updated = [...topics];
    updated[index][field] = value;
    setTopics(updated);
  };

  const updateValRule = (index, field, value) => {
    const updated = [...valRules];
    updated[index][field] = value;
    setValRules(updated);
  };

  const deleteTopic = (index) => {
    const updated = topics.filter((_, i) => i !== index);
    setTopics(updated);
    if (editingTopicIndex === index) {
      setEditingTopicIndex(null);
    }
  };

  const deleteValRule = (index) => {
    const updated = valRules.filter((_, i) => i !== index);
    setValRules(updated);
    if (editingValRuleIndex === index) {
      setEditingValRuleIndex(null);
    }
  };

  const handleSaveTopic = (index) => {
    const topic = topics[index];
    const errors = { ...topicErrors };

    if (topic.name.trim() === '' || !validatePriority(topic.priority)) {
      errors[index] = 'Please fill valid Topic Name and Priority (1 - ' + maxPriority + ')';
      setTopicErrors(errors);
      return;
    }

    delete errors[index];
    setTopicErrors(errors);
    setEditingTopicIndex(null);
  };

  const handleSaveValRule = (index) => {
    const rule = valRules[index];
    const errors = { ...valRuleErrors };

    if (rule.val.trim() === '' || !validatePriority(rule.priority)) {
      errors[index] = 'Please fill valid Value and Priority (1 - ' + maxPriority + ')';
      setValRuleErrors(errors);
      return;
    }

    delete errors[index];
    setValRuleErrors(errors);
    setEditingValRuleIndex(null);
  };

  const handleSubmit = () => {
    if (!defaultPriority || !maxPriority) {
      setErrorMessage('Default and Max Priority are required.');
      return;
    }

    const hasInvalidTopics = topics.some(t => t.name.trim() === '' || !validatePriority(t.priority));
    const hasInvalidValRules = valRules.some(v => v.val.trim() === '' || !validatePriority(v.priority));

    if (hasInvalidTopics || hasInvalidValRules) {
      setErrorMessage('Some settings are invalid. Please fix them before saving.');
      return;
    }

    const settings = {
      topics: topics.filter(t => t.name && t.priority),
      defaultPriority,
      priorityRange: { min: "1", max: maxPriority },
      valRules: valRules.filter(v => v.val && v.priority),
    };

    const fileData = JSON.stringify(settings, null, 2);
    const blob = new Blob([fileData], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'user-settings.json';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);

    setSuccessFullScreen(true);
    setTimeout(() => {
      setSuccessFullScreen(false);
      setSuccessMessage('Settings saved successfully!');
    }, 3000);
    setErrorMessage('');
  };

  if (successFullScreen) {
    return (
      <div className="success-fullscreen">
        <h1 className="success-big-text">Settings Saved Successfully!</h1>
        <img src={rocketIcon} alt="rocket" className="rocket-animation-only" />
      </div>
    );
  }

  return (
    <div className="page-container">
      <img src={logo} alt="KafkaBoost Logo" className="logo" />
      <div className="form-container">
        {errorMessage && <div className="error-message">{errorMessage}</div>}
        {successMessage && <div className="success-message">{successMessage}</div>}

        {stage === 'defineMax' ? (
          <>
            <h1>Define Priorities</h1>
            <input
              placeholder="Enter Default Priority"
              value={defaultPriority}
              onChange={(e) => setDefaultPriority(e.target.value)}
              className="input-style"
            />
            <input
              placeholder="Enter Max Priority"
              value={maxPriority}
              onChange={(e) => setMaxPriority(e.target.value)}
              className="input-style"
            />
            <ButtonWithIcon onClick={handleMaxPrioritySubmit} text="Continue" />
          </>
        ) : (
          <>
            <h2>Settings</h2>
            {editingSettings ? (
              <div style={{ marginBottom: '20px' }}>
                <input
                  value={defaultPriority}
                  onChange={(e) => setDefaultPriority(e.target.value)}
                  placeholder="Default Priority"
                  className={`input-style ${(defaultPriority.trim() === '' || isNaN(defaultPriority) || parseInt(defaultPriority) > parseInt(maxPriority)) ? 'input-error' : ''}`}
                />
                <input
                  value={maxPriority}
                  onChange={(e) => setMaxPriority(e.target.value)}
                  placeholder="Max Priority"
                  className={`input-style ${(maxPriority.trim() === '' || isNaN(maxPriority) || parseInt(maxPriority) < parseInt(defaultPriority)) ? 'input-error' : ''}`}
                />
                <button
                  className="save-button"
                  onClick={() => {
                    if (
                      defaultPriority.trim() === '' ||
                      maxPriority.trim() === '' ||
                      isNaN(defaultPriority) ||
                      isNaN(maxPriority) ||
                      parseInt(defaultPriority) > parseInt(maxPriority)
                    ) {
                      alert('Please enter valid settings: Default Priority must be <= Max Priority.');
                      return;
                    }
                    setEditingSettings(false);
                  }}
                >
                  Save Settings
                </button>
              </div>
            ) : (
              <div style={{ marginBottom: '20px' }}>
                <p><b>Default Priority:</b> {defaultPriority}</p>
                <p><b>Max Priority:</b> {maxPriority}</p>
                <button className="edit-button" onClick={() => setEditingSettings(true)}>Edit Settings</button>
              </div>
            )}

            {/* Topics Table */}
            <div className="table-header">
            <div style={{ display: 'flex', alignItems: 'center' }}>
            <h2 style={{ margin: 0 }}>Topics</h2>
            <button className="plus-button" onClick={addTopic}>➕</button>
  </div>
</div>

            <table className="styled-table">
              <thead>
                <tr>
                  <th>Topic Name</th>
                  <th>Priority</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {topics.map((topic, index) => (
                  <tr key={index}>
                    {editingTopicIndex === index ? (
                      <>
                        <td>
                          <input
                            value={topic.name}
                            onChange={(e) => updateTopic(index, 'name', e.target.value)}
                            className={`input-style ${topic.name.trim() === '' ? 'input-error' : ''}`}
                          />
                        </td>
                        <td>
                          <input
                            value={topic.priority}
                            onChange={(e) => updateTopic(index, 'priority', e.target.value)}
                            className={`input-style ${!validatePriority(topic.priority) ? 'input-error' : ''}`}
                          />
                        </td>
                        <td>
                          <button className="save-button" onClick={() => handleSaveTopic(index)}>Save</button>
                          <button className="delete-button" onClick={() => deleteTopic(index)}>Delete</button>
                          {topicErrors[index] && <div className="inline-error">{topicErrors[index]}</div>}
                        </td>
                      </>
                    ) : (
                      <>
                        <td>{topic.name}</td>
                        <td>{topic.priority}</td>
                        <td><button className="edit-button" onClick={() => setEditingTopicIndex(index)}>Edit</button></td>
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Value Rules Table */}
            <div className="table-header">
              <h2>Value-Based Rules</h2>
              <button className="plus-button" onClick={addValRule}>➕</button>
            </div>
            <table className="styled-table">
              <thead>
                <tr>
                  <th>Value</th>
                  <th>Priority</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {valRules.map((rule, index) => (
                  <tr key={index}>
                    {editingValRuleIndex === index ? (
                      <>
                        <td>
                          <input
                            value={rule.val}
                            onChange={(e) => updateValRule(index, 'val', e.target.value)}
                            className={`input-style ${rule.val.trim() === '' ? 'input-error' : ''}`}
                          />
                        </td>
                        <td>
                          <input
                            value={rule.priority}
                            onChange={(e) => updateValRule(index, 'priority', e.target.value)}
                            className={`input-style ${!validatePriority(rule.priority) ? 'input-error' : ''}`}
                          />
                        </td>
                        <td>
                          <button className="save-button" onClick={() => handleSaveValRule(index)}>Save</button>
                          <button className="delete-button" onClick={() => deleteValRule(index)}>Delete</button>
                          {valRuleErrors[index] && <div className="inline-error">{valRuleErrors[index]}</div>}
                        </td>
                      </>
                    ) : (
                      <>
                        <td>{rule.val}</td>
                        <td>{rule.priority}</td>
                        <td><button className="edit-button" onClick={() => setEditingValRuleIndex(index)}>Edit</button></td>
                      </>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>

            {/* Save All Button */}
            <div style={{ marginTop: '30px' }}>
              <ButtonWithIcon onClick={handleSubmit} text="Save All Settings" />
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function ButtonWithIcon({ onClick, text }) {
  return (
    <button onClick={onClick} className="button-style">
      <img src={rocketIcon} alt="rocket" style={{ width: '20px', height: '20px' }} />
      {text}
    </button>
  );
}

export default App;
