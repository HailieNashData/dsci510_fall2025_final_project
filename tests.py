import unittest
import sys
import os
from data_collection import F1DataCollector
import pandas as pd


class TestF1DataCollector(unittest.TestCase):
    """Test cases for F1DataCollector class"""
    
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
    
    def test_02_openf1_drivers(self):
        """Test: Fetch driver data from OpenF1 API"""
        print("\n[TEST 2] Testing OpenF1drivers endpoint...")
        
        # First get a valid session key
        sessions = self.collector.get_sessions(2024)
        if not sessions.empty:
            session_key = sessions.iloc[0]['session_key']
            
            drivers = self.collector.get_drivers(session_key)
            
            self.assertIsInstance(drivers, pd.DataFrame, "Should return a DataFrame")
            self.assertFalse(drivers.empty, "Drivers data should not be empty")
            self.assertIn('driver_number', drivers.columns, "Should have driver_number column")
            self.assertIn('full_name', drivers.columns, "Should have full_name column")
            
            print(f"✓ Retrieved {len(drivers)} drivers")
            print(f"✓ Sample drivers: {drivers['full_name'].head(3).tolist()}")
    
    def test_03_openf1_laps(self):
        """Test: Fetch lap data from OpenF1 API"""
        print("\n[TEST 3] Testing OpenF1 laps endpoint...")
        
        # Get a race session
        sessions = self.collector.get_sessions(2024)
        race_sessions = sessions[sessions['session_name'] == 'Race']
        
        if not race_sessions.empty:
            session_key = race_sessions.iloc[0]['session_key']
            
            # Get laps for first driver only to limit data
            laps = self.collector.get_laps(session_key, driver_number=1)
            
            self.assertIsInstance(laps, pd.DataFrame, "Should return a DataFrame")
            self.assertIn('lap_duration', laps.columns, "Should have lap_duration column")
            self.assertIn('lap_number', laps.columns, "Should have lap_number column")
            
            if not laps.empty:
                print(f"✓ Retrieved {len(laps)} laps")
                print(f"✓ Average lap duration: {laps['lap_duration'].mean():.2f} seconds")
    
    def test_04_openf1_pit_stops(self):
        """Test: Fetch pit stop data from OpenF1 API"""
        print("\n[TEST 4] Testing OpenF1 pit stops endpoint...")
        
        sessions = self.collector.get_sessions(2024)
        race_sessions = sessions[sessions['session_name'] == 'Race']
        
        if not race_sessions.empty:
            session_key = race_sessions.iloc[0]['session_key']
            
            pit_stops = self.collector.get_pit_stops(session_key)
            
            self.assertIsInstance(pit_stops, pd.DataFrame, "Should return a DataFrame")
            
            if not pit_stops.empty:
                self.assertIn('pit_duration', pit_stops.columns, "Should have pit_duration column")
                print(f"✓ Retrieved {len(pit_stops)} pit stops")
                print(f"✓ Average pit duration: {pit_stops['pit_duration'].mean():.2f} seconds")
            else:
                print("⚠ No pit stop data available for this session")
    
    def test_05_ergast_race_results(self):
        """Test: Fetch race results from Ergast API"""
        print("\n[TEST 5] Testing Ergast race results endpoint...")
        
        results = self.collector.get_race_results(2024)
        
        self.assertIsInstance(results, pd.DataFrame, "Should return a DataFrame")
        self.assertFalse(results.empty, "Race results should not be empty")
        self.assertIn('position', results.columns, "Should have position column")
        self.assertIn('driver_name', results.columns, "Should have driver_name column")
        self.assertIn('team', results.columns, "Should have team column")
        self.assertIn('points', results.columns, "Should have points column")
        
        print(f"✓ Retrieved {len(results)} race results")
        print(f"✓ Number of races: {results['round'].nunique()}")
        print(f"✓ Sample teams: {results['team'].unique()[:3].tolist()}")
    