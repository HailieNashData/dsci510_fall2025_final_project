"""
Test suite for F1 data collection
Run with: python tests.py
"""

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
    
    def test_06_ergast_qualifying(self):
        """Test: Fetch qualifying results from Ergast API"""
        print("\n[TEST 6] Testing Ergast qualifying endpoint...")
        
        quali = self.collector.get_qualifying_results(2024)
        
        self.assertIsInstance(quali, pd.DataFrame, "Should return a DataFrame")
        self.assertFalse(quali.empty, "Qualifying results should not be empty")
        self.assertIn('position', quali.columns, "Should have position column")
        self.assertIn('Q1', quali.columns, "Should have Q1 column")
        
        print(f"✓ Retrieved {len(quali)} qualifying results")
        print(f"✓ Drivers with Q3 times: {quali['Q3'].notna().sum()}")
    
    def test_07_ergast_standings(self):
        """Test: Fetch driver standings from Ergast API"""
        print("\n[TEST 7] Testing Ergast driver standings endpoint...")
        
        standings = self.collector.get_driver_standings(2024)
        
        self.assertIsInstance(standings, pd.DataFrame, "Should return a DataFrame")
        self.assertFalse(standings.empty, "Standings should not be empty")
        self.assertIn('points', standings.columns, "Should have points column")
        self.assertIn('wins', standings.columns, "Should have wins column")
        
        # Get final standings (last round)
        final_round = standings['round'].max()
        final_standings = standings[standings['round'] == final_round].sort_values('position')
        
        print(f"✓ Retrieved standings for {standings['round'].nunique()} rounds")
        if not final_standings.empty:
            print(f"✓ Championship leader: {final_standings.iloc[0]['driver_name']} ({final_standings.iloc[0]['points']} points)")
    
    def test_08_data_consistency(self):
        """Test: Check data consistency between APIs"""
        print("\n[TEST 8] Testing data consistency between APIs...")
        
        # Get data from both sources
        ergast_results = self.collector.get_race_results(2024)
        ergast_quali = self.collector.get_qualifying_results(2024)
        
        # Check race counts match
        ergast_race_count = ergast_results['round'].nunique()
        ergast_quali_count = ergast_quali['round'].nunique()
        
        self.assertEqual(ergast_race_count, ergast_quali_count, 
                        "Number of races should match between results and qualifying")
        
        print(f"✓ Data consistency verified: {ergast_race_count} races in both datasets")
    
    def test_09_error_handling(self):
        """Test: API error handling with invalid requests"""
        print("\n[TEST 9] Testing error handling...")
        
        # Test with invalid session key
        invalid_laps = self.collector.get_laps(session_key=999999)
        self.assertIsInstance(invalid_laps, pd.DataFrame, "Should return empty DataFrame on error")
        
        # Test with future year (should return empty or limited data)
        future_data = self.collector.get_race_results(2030)
        self.assertIsInstance(future_data, pd.DataFrame, "Should handle future year gracefully")
        
        print("✓ Error handling works correctly")
    
    def test_10_data_types(self):
        """Test: Verify correct data types in collected data"""
        print("\n[TEST 10] Testing data types...")
        
        results = self.collector.get_race_results(2024)
        
        if not results.empty:
            # Check that numeric columns are properly typed
            self.assertTrue(pd.api.types.is_numeric_dtype(results['points']), 
                          "Points should be numeric")
            
            # Check that position can be converted to numeric (may have strings like 'R' for retired)
            position_numeric = pd.to_numeric(results['position'], errors='coerce')
            self.assertTrue(position_numeric.notna().any(), 
                          "Should have some valid numeric positions")
            
            print("✓ Data types are correct")
            print(f"✓ Points range: {results['points'].min()} to {results['points'].max()}")


class TestDataSaving(unittest.TestCase):
    """Test cases for data saving functionality"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures"""
        cls.collector = F1DataCollector()
        cls.test_dir = "test_data"
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test files"""
        import shutil
        if os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir)
    
    def test_save_to_csv(self):
        """Test: Save data to CSV file"""
        print("\n[TEST 11] Testing CSV export...")
        
        # Get sample data
        sessions = self.collector.get_sessions(2024)
        
        if not sessions.empty:
            # Save to test directory
            self.collector.save_to_csv(sessions.head(10), "test_sessions.csv", self.test_dir)
            
            # Verify file exists
            filepath = os.path.join(self.test_dir, "test_sessions.csv")
            self.assertTrue(os.path.exists(filepath), "CSV file should be created")
            
            # Verify we can read it back
            df = pd.read_csv(filepath)
            self.assertEqual(len(df), 10, "Should have saved 10 records")
            
            print(f"✓ Successfully saved and verified CSV file")
    
    def test_save_to_json(self):
        """Test: Save data to JSON file"""
        print("\n[TEST 12] Testing JSON export...")
        
        results = self.collector.get_race_results(2024)
        
        if not results.empty:
            # Save to test directory
            self.collector.save_to_json(results.head(10), "test_results.json", self.test_dir)
            
            # Verify file exists
            filepath = os.path.join(self.test_dir, "test_results.json")
            self.assertTrue(os.path.exists(filepath), "JSON file should be created")
            
            # Verify we can read it back
            df = pd.read_json(filepath)
            self.assertEqual(len(df), 10, "Should have saved 10 records")
            
            print(f"✓ Successfully saved and verified JSON file")


def run_tests():
    """Run all tests and print summary"""
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestF1DataCollector))
    suite.addTests(loader.loadTestsFromTestCase(TestDataSaving))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print("="*60 + "\n")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)