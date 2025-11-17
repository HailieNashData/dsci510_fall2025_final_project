import unittest
import sys
import os
from data_collection import F1DataCollector
import pandas as pd

class TestF1DataCollector(unittest.TestCase):
  @classmethod
    def setUpClass(cls):
        """Set up test fixtures - runs once before all tests"""
        print("\n" + "="*60)
        print("Starting F1 Data Collection Tests")
        print("="*60 + "\n")
        cls.collector = F1DataCollector()
    
    def test_01_openf1_sessions(self):
        """Test: Fetch sessions from OpenF1 API"""
        print("\n[TEST 1] Testing OpenF1 sessions endpoint...")
        
        sessions = self.collector.get_sessions(2024)
        
        self.assertIsInstance(sessions, pd.DataFrame, "Should return a DataFrame")
        self.assertFalse(sessions.empty, "Sessions data should not be empty")
        self.assertIn('session_key', sessions.columns, "Should have session_key column")
        self.assertIn('session_name', sessions.columns, "Should have session_name column")
        
        print(f"✓ Retrieved {len(sessions)} sessions")
        print(f"✓ Columns: {', '.join(sessions.columns.tolist()[:5])}...")
        print(f"✓ Session types: {sessions['session_name'].unique().tolist()}")
    
