#!/usr/bin/env python3
"""
Extract First and Second Division Leagues from Target Countries
Creates a markdown file with league information for specified countries
"""

import requests
import pandas as pd
import json
from typing import Dict, List, Any

# Target countries and their IDs (from countries_football.md)
TARGET_COUNTRIES = {
    'England': 1,
    'Spain': 32, 
    'Germany': 30,
    'Italy': 31,
    'France': 7,
    'Portugal': 44,
    'Poland': 47,
    'Norway': 5,
    'Sweden': 9,
    'Denmark': 8,
    'Austria': 17,
    'Czech Republic': 18,
    'Netherlands': 35,  # Holland
    'Belgium': 33,
    'Turkey': 46,
    'Brazil': 13,
    'Argentina': 48,
    'Switzerland': 25,
    'Greece': 67
}

def get_leagues_for_country(country_id: int, api_key: str) -> List[Dict[str, Any]]:
    """
    Get tournaments/leagues for a specific country
    """
    url = "https://sofascore.p.rapidapi.com/tournaments/list"
    
    querystring = {"categoryId": str(country_id)}
    
    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": "sofascore.p.rapidapi.com"
    }
    
    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        data = response.json()
        
        # Extract unique tournaments from the first group (usually main leagues)
        if 'groups' in data and len(data['groups']) > 0:
            # Get the first group which typically contains the main leagues
            first_group = data['groups'][0]
            if 'uniqueTournaments' in first_group:
                return first_group['uniqueTournaments']
        
        return []
        
    except Exception as e:
        print(f"Error getting leagues for country ID {country_id}: {e}")
        return []

def extract_top_leagues():
    """
    Extract first and second division leagues from target countries
    """
    # Use the API key from the notebook
    api_key = "23326581afmsha80927a889f5d69p1c1e08jsnb6e742b99b4a"
    
    all_leagues = []
    
    for country_name, country_id in TARGET_COUNTRIES.items():
        print(f"Processing {country_name} (ID: {country_id})...")
        
        leagues = get_leagues_for_country(country_id, api_key)
        
        # Take first 2 leagues (typically first and second division)
        top_leagues = leagues[:2] if len(leagues) >= 2 else leagues
        
        for i, league in enumerate(top_leagues, 1):
            league_info = {
                'country': country_name,
                'country_id': country_id,
                'division': f"{i}st Division" if i == 1 else f"{i}nd Division",
                'league_name': league.get('name', 'Unknown'),
                'league_id': league.get('id', 'Unknown'),
                'slug': league.get('slug', 'Unknown'),
                'has_standings_groups': league.get('hasStandingsGroups', False),
                'has_rounds': league.get('hasRounds', False),
                'display_name': league.get('displayName', league.get('name', 'Unknown'))
            }
            all_leagues.append(league_info)
    
    return all_leagues

def create_leagues_md(leagues: List[Dict[str, Any]]):
    """
    Create markdown file with league information
    """
    md_content = """# First and Second Division Football Leagues

| Country | Division | League Name | League ID | Slug |
|---------|----------|-------------|-----------|------|
"""
    
    for league in leagues:
        md_content += f"| {league['country']} | {league['division']} | {league['league_name']} | {league['league_id']} | {league['slug']} |\n"
    
    # Write to file
    with open('leagues_top_divisions.md', 'w', encoding='utf-8') as f:
        f.write(md_content)
    
    print(f"Created leagues_top_divisions.md with {len(leagues)} leagues")

def create_detailed_json(leagues: List[Dict[str, Any]]):
    """
    Create detailed JSON file with all league information
    """
    output = {
        'generated_at': str(pd.Timestamp.now()),
        'total_leagues': len(leagues),
        'leagues_by_country': {},
        'all_leagues': leagues
    }
    
    # Group by country
    for league in leagues:
        country = league['country']
        if country not in output['leagues_by_country']:
            output['leagues_by_country'][country] = []
        output['leagues_by_country'][country].append(league)
    
    # Write to file
    with open('leagues_top_divisions.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"Created leagues_top_divisions.json with detailed information")

if __name__ == "__main__":
    print("Extracting top division leagues from target countries...")
    
    leagues = extract_top_leagues()
    
    if leagues:
        create_leagues_md(leagues)
        create_detailed_json(leagues)
        
        print(f"\nSummary:")
        print(f"Total leagues extracted: {len(leagues)}")
        
        # Print summary by country
        countries_count = {}
        for league in leagues:
            country = league['country']
            countries_count[country] = countries_count.get(country, 0) + 1
        
        for country, count in countries_count.items():
            print(f"{country}: {count} leagues")
    else:
        print("No leagues were extracted!")
