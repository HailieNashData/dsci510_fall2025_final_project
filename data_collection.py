import requests
import json
import time
import pandas as pd
from typing import Dict, List, Optional
import os

class F1DataCollector: 
    def __init__(self): 
        self.openf1_base_url = "https://api.openf1.org/v1"
        self.ergast_base_url = "http://ergast.com/api/f1"
        self.session = requests.Session()
    
    def _make_request(self,url: str, params: Optional[Dict] = None,
                       max_retries: int = 3) -> Optional[Dict]:
        """
            Make HTTP request with retry logic
        
        Args:
            url: API endpoint URL
            params: Query parameters
            max_retries: Maximum number of retry attempts
            
        Returns:
            JSON response as dictionary or None if failed
        """

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=10)
                response.raise_for_status()
                
                # Handle both JSON and plain text responses
                if 'application/json' in response.headers.get('Content-Type', ''):
                    return response.json()
                else:
                    return json.loads(response.text)
                    
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"Failed to fetch data from {url}")
                    return None
        return None
    def get_sessions(self, year: int) -> pd.DataFrame:
       
        url = f"{self.openf1_base_url}/sessions"
        params = {"year": year}
        
        data = self._make_request(url, params)
        if data:
            df = pd.DataFrame(data)
            print(f"Retrieved {len(df)} sessions for {year}")
            return df
        return pd.DataFrame()
    
    def get_laps(self, session_key: int, driver_number: Optional[int] = None) -> pd.DataFrame:
      
        url = f"{self.openf1_base_url}/laps"
        params = {"session_key": session_key}
        
        if driver_number:
            params["driver_number"] = driver_number
        
        data = self._make_request(url, params)
        if data:
            df = pd.DataFrame(data)
            print(f"Retrieved {len(df)} laps for session {session_key}")
            return df
        return pd.DataFrame()
    
    def get_pit_stops(self, session_key: int) -> pd.DataFrame:
        """
        Get pit stop data for a specific session
        
        Args:
            session_key: Unique session identifier
            
        Returns:
            DataFrame with pit stop information
        """
        url = f"{self.openf1_base_url}/pit"
        params = {"session_key": session_key}
        
        data = self._make_request(url, params)
        if data:
            df = pd.DataFrame(data)
            print(f"Retrieved {len(df)} pit stops for session {session_key}")
            return df
        return pd.DataFrame()
    
    def get_drivers(self, session_key: int) -> pd.DataFrame:
        """
        Get driver information for a specific session
        
        Args:
            session_key: Unique session identifier
            
        Returns:
            DataFrame with driver details
        """
        url = f"{self.openf1_base_url}/drivers"
        params = {"session_key": session_key}
        
        data = self._make_request(url, params)
        if data:
            df = pd.DataFrame(data)
            print(f"Retrieved {len(df)} drivers for session {session_key}")
            return df
        return pd.DataFrame()
    def get_race_results(self, year: int) -> pd.DataFrame:
        """
        Get race results for all races in a season
        
        Args:
            year: Season year
            
        Returns:
            DataFrame with race results
        """
        url = f"{self.ergast_base_url}/{year}/results.json"
        params = {"limit": 1000}  # Ensure we get all results
        
        data = self._make_request(url, params)
        if data and 'MRData' in data:
            races = data['MRData']['RaceTable']['Races']
            
            results_list = []
            for race in races:
                race_name = race['raceName']
                circuit = race['Circuit']['circuitName']
                date = race['date']
                round_num = race['round']
                
                for result in race['Results']:
                    result_dict = {
                        'season': year,
                        'round': round_num,
                        'race_name': race_name,
                        'circuit': circuit,
                        'date': date,
                        'position': result.get('position'),
                        'driver_id': result['Driver']['driverId'],
                        'driver_name': f"{result['Driver']['givenName']} {result['Driver']['familyName']}",
                        'driver_number': result.get('number'),
                        'team': result['Constructor']['name'],
                        'grid': result.get('grid'),
                        'points': result.get('points'),
                        'status': result.get('status'),
                        'fastest_lap_rank': result.get('FastestLap', {}).get('rank'),
                        'fastest_lap_time': result.get('FastestLap', {}).get('Time', {}).get('time')
                    }
                    results_list.append(result_dict)
            
            df = pd.DataFrame(results_list)
            print(f"Retrieved {len(df)} race results for {year}")
            return df
        return pd.DataFrame()
    
    def get_qualifying_results(self, year: int) -> pd.DataFrame:
        """
        Get qualifying results for all races in a season
        
        Args:
            year: Season year
            
        Returns:
            DataFrame with qualifying results
        """
        url = f"{self.ergast_base_url}/{year}/qualifying.json"
        params = {"limit": 1000}
        
        data = self._make_request(url, params)
        if data and 'MRData' in data:
            races = data['MRData']['RaceTable']['Races']
            
            quali_list = []
            for race in races:
                race_name = race['raceName']
                round_num = race['round']
                
                for result in race['QualifyingResults']:
                    quali_dict = {
                        'season': year,
                        'round': round_num,
                        'race_name': race_name,
                        'position': result.get('position'),
                        'driver_id': result['Driver']['driverId'],
                        'driver_name': f"{result['Driver']['givenName']} {result['Driver']['familyName']}",
                        'driver_number': result.get('number'),
                        'team': result['Constructor']['name'],
                        'Q1': result.get('Q1'),
                        'Q2': result.get('Q2'),
                        'Q3': result.get('Q3')
                    }
                    quali_list.append(quali_dict)
            
            df = pd.DataFrame(quali_list)
            print(f"Retrieved {len(df)} qualifying results for {year}")
            return df
        return pd.DataFrame()
    
    def get_driver_standings(self, year: int) -> pd.DataFrame:
        """
        Get final driver championship standings for a season
        
        Args:
            year: Season year
            
        Returns:
            DataFrame with driver standings
        """
        url = f"{self.ergast_base_url}/{year}/driverStandings.json"
        
        data = self._make_request(url, params={"limit": 1000})
        if data and 'MRData' in data:
            standings_list = data['MRData']['StandingsTable']['StandingsLists']
            
            all_standings = []
            for standing in standings_list:
                round_num = standing.get('round')
                for driver_standing in standing['DriverStandings']:
                    standing_dict = {
                        'season': year,
                        'round': round_num,
                        'position': driver_standing['position'],
                        'driver_id': driver_standing['Driver']['driverId'],
                        'driver_name': f"{driver_standing['Driver']['givenName']} {driver_standing['Driver']['familyName']}",
                        'team': driver_standing['Constructors'][0]['name'],
                        'points': driver_standing['points'],
                        'wins': driver_standing['wins']
                    }
                    all_standings.append(standing_dict)
            
            df = pd.DataFrame(all_standings)
            print(f"Retrieved {len(df)} driver standings entries for {year}")
            return df
        return pd.DataFrame()
    
    # ===== Data Saving Methods =====
    
    def save_to_json(self, data: pd.DataFrame, filename: str, directory: str = "data"):
        """
        Save DataFrame to JSON file
        
        Args:
            data: DataFrame to save
            filename: Output filename
            directory: Output directory
        """
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, filename)
        data.to_json(filepath, orient='records', indent=2)
        print(f"Saved {len(data)} records to {filepath}")
    
    def save_to_csv(self, data: pd.DataFrame, filename: str, directory: str = "data"):
        """
        Save DataFrame to CSV file
        
        Args:
            data: DataFrame to save
            filename: Output filename
            directory: Output directory
        """
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, filename)
        data.to_csv(filepath, index=False)
        print(f"Saved {len(data)} records to {filepath}")


def collect_season_data(year: int, collector: F1DataCollector):
    """
    Collect all data for a specific season
    
    Args:
        year: Season year to collect
        collector: F1DataCollector instance
    """
    print(f"\n{'='*50}")
    print(f"Collecting data for {year} season")
    print(f"{'='*50}\n")
    
    # Collect from Ergast API
    print("Fetching Ergast API data...")
    race_results = collector.get_race_results(year)
    quali_results = collector.get_qualifying_results(year)
    standings = collector.get_driver_standings(year)
    
    # Save Ergast data
    if not race_results.empty:
        collector.save_to_csv(race_results, f"race_results_{year}.csv")
    if not quali_results.empty:
        collector.save_to_csv(quali_results, f"qualifying_{year}.csv")
    if not standings.empty:
        collector.save_to_csv(standings, f"standings_{year}.csv")
    
    # Collect from OpenF1 API
    print("\nFetching OpenF1 API data...")
    sessions = collector.get_sessions(year)
    
    if not sessions.empty:
        collector.save_to_csv(sessions, f"sessions_{year}.csv")
        
        # Get race sessions only (not practice or qualifying)
        race_sessions = sessions[sessions['session_name'] == 'Race']
        print(f"\nFound {len(race_sessions)} race sessions")
        
        # Collect sample lap data from first 3 races to avoid overwhelming API
        for idx, session in race_sessions.head(3).iterrows():
            session_key = session['session_key']
            print(f"\nCollecting data for session {session_key}...")
            
            laps = collector.get_laps(session_key)
            if not laps.empty:
                collector.save_to_csv(laps, f"laps_{year}_session_{session_key}.csv")
            
            pit_stops = collector.get_pit_stops(session_key)
            if not pit_stops.empty:
                collector.save_to_csv(pit_stops, f"pit_stops_{year}_session_{session_key}.csv")
            
            time.sleep(1)  # Rate limiting
    
    print(f"\n{'='*50}")
    print(f"Completed data collection for {year}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    # Initialize collector
    collector = F1DataCollector()
    
    # Collect data for 2023 and 2024 seasons
    for year in [2023, 2024]:
        collect_season_data(year, collector)
    
    print("\n Data collection complete!")