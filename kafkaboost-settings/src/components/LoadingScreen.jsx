import React from 'react';
import './LoadingScreen.css';
import logo from '../assets/kafkaboost-logo.png'; 

function LoadingScreen() {
  return (
    <div className="loading-screen">
      <img src={logo} alt="KafkaBoost Logo" className="loading-logo" />
      <h2>Loading...</h2>
    </div>
  );
}

export default LoadingScreen;
