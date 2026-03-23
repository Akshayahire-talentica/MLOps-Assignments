#!/usr/bin/env python3
"""
User Behavior Simulator
========================

Simulates realistic user interactions with the recommendation system.
Generates synthetic events for testing and training the feedback loop.

Features:
- Rank-based click probability (position bias)
- Realistic watch-time distributions
- Positive and negative feedback generation
- Session-based behavior modeling
- Flags all events as synthetic

Usage:
    python scripts/simulate_user_behavior_v2.py --users 100 --sessions 1000
"""

import argparse
import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import numpy as np
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

API_URL = "http://localhost:8000"
EVENT_COLLECTOR_URL = "http://localhost:8002"

# Movie duration distribution (minutes)
MOVIE_DURATIONS = {
    'short': (80, 100),      # 80-100 min
    'medium': (100, 130),    # 100-130 min
    'long': (130, 180)       # 130-180 min
}

# User personas with different behaviors
USER_PERSONAS = {
    'engaged': {        # Highly engaged users
        'weight': 0.2,
        'click_base_prob': 0.4,
        'watch_prob': 0.8,
        'completion_mean': 0.75,
        'like_prob': 0.4,
        'dislike_prob': 0.05
    },
    'casual': {         # Casual browsers
        'weight': 0.5,
        'click_base_prob': 0.2,
        'watch_prob': 0.5,
        'completion_mean': 0.45,
        'like_prob': 0.15,
        'dislike_prob': 0.1
    },
    'browser': {        # Just browsing, low engagement
        'weight': 0.2,
        'click_base_prob': 0.15,
        'watch_prob': 0.3,
        'completion_mean': 0.25,
        'like_prob': 0.05,
        'dislike_prob': 0.2
    },
    'picky': {          # Picky users, skip a lot
        'weight': 0.1,
        'click_base_prob': 0.25,
        'watch_prob': 0.4,
        'completion_mean': 0.35,
        'like_prob': 0.25,
        'dislike_prob': 0.3
    }
}

# ============================================================
# BEHAVIOR MODELS
# ============================================================

class UserBehaviorSimulator:
    """Simulates realistic user behavior patterns"""
    
    def __init__(self, api_url: str, event_collector_url: str):
        self.api_url = api_url
        self.event_collector_url = event_collector_url
        self.session = requests.Session()
        
    def assign_persona(self) -> Dict:
        """Assign a user persona based on weights"""
        personas = list(USER_PERSONAS.keys())
        weights = [USER_PERSONAS[p]['weight'] for p in personas]
        persona_name = random.choices(personas, weights=weights)[0]
        return {'name': persona_name, **USER_PERSONAS[persona_name]}
    
    def calculate_click_probability(self, rank: int, base_prob: float) -> float:
        """
        Calculate click probability with position bias
        Higher ranked items are more likely to be clicked
        
        Position bias follows a power law: P(click|rank) ∝ 1/rank^α
        """
        alpha = 0.5  # Position bias exponent
        position_factor = 1.0 / (rank ** alpha)
        # Normalize: rank 1 has full base_prob, rank 10 has ~30% of base_prob
        click_prob = base_prob * (position_factor / (1.0 ** alpha))
        return min(click_prob, 1.0)
    
    def generate_watch_duration(self, total_duration: int, completion_mean: float) -> int:
        """
        Generate realistic watch duration
        
        Uses beta distribution for completion rate:
        - Most users either watch very little or complete the content
        - Few users stop in the middle
        """
        # Beta distribution parameters for bimodal behavior
        if completion_mean > 0.6:
            # Engaged users: concentrate around high completion
            alpha, beta = 5, 2
        elif completion_mean > 0.4:
            # Casual users: uniform to slightly positive
            alpha, beta = 2, 2
        else:
            # Browsers: concentrate around low completion
            alpha, beta = 2, 5
        
        completion_rate = np.random.beta(alpha, beta)
        
        # Add some noise
        completion_rate = max(0.05, min(1.0, completion_rate))
        
        watch_duration = int(total_duration * completion_rate)
        return watch_duration
    
    def decide_feedback(self, completion_rate: float, persona: Dict) -> Optional[str]:
        """
        Decide if user gives feedback based on watch behavior
        
        Feedback more likely if:
        - High completion (liked it)
        - Very low completion (didn't like it)
        """
        # High completion → likely positive feedback
        if completion_rate > 0.7:
            if random.random() < persona['like_prob']:
                return 'like' if random.random() < 0.9 else 'rating'
        
        # Very low completion → likely negative feedback
        elif completion_rate < 0.2:
            if random.random() < persona['dislike_prob']:
                return 'not_interested' if random.random() < 0.7 else 'dislike'
        
        # Medium completion → less likely to give feedback
        elif random.random() < 0.05:
            return random.choice(['like', 'dislike', 'not_interested'])
        
        return None
    
    def simulate_session(self, user_id: int, persona: Dict, session_id: str) -> Dict:
        """
        Simulate a complete user session
        
        Flow:
        1. Request recommendations
        2. Decide which items to click (position bias)
        3. For clicked items: watch for some duration
        4. Optionally provide feedback
        """
        stats = {
            'impressions': 0,
            'clicks': 0,
            'watches': 0,
            'feedback': 0,
            'errors': 0
        }
        
        try:
            # Step 1: Get recommendations
            response = self.session.get(
                f"{self.api_url}/recommend",
                params={'user_id': user_id, 'top_k': 10},
                timeout=10
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to get recommendations: {response.status_code}")
                stats['errors'] += 1
                return stats
            
            rec_data = response.json()
            recommendation_id = rec_data['recommendation_id']
            recommendations = rec_data['recommendations']
            model_version = rec_data.get('model_version', 'unknown')
            
            # Log impression event
            impression_event = {
                'event_type': 'impression',
                'recommendation_id': recommendation_id,
                'user_id': user_id,
                'item_ids': [r['item_id'] for r in recommendations],
                'scores': [r['score'] for r in recommendations],
                'model_name': 'nmf_model',
                'model_version': model_version,
                'is_synthetic': True,
                'context': {'session_id': session_id, 'persona': persona['name']}
            }
            
            response = self.session.post(
                f"{self.event_collector_url}/events/impression",
                json=impression_event,
                timeout=5
            )
            
            if response.status_code == 200:
                stats['impressions'] += 1
            else:
                logger.warning(f"Failed to log impression: {response.status_code}")
                stats['errors'] += 1
            
            # Step 2: Decide which items to click
            for rec in recommendations:
                rank = rec['rank']
                item_id = rec['item_id']
                
                # Calculate click probability with position bias
                click_prob = self.calculate_click_probability(rank, persona['click_base_prob'])
                
                if random.random() < click_prob:
                    # User clicks this item
                    click_event = {
                        'event_type': 'click',
                        'recommendation_id': recommendation_id,
                        'user_id': user_id,
                        'item_id': item_id,
                        'position': rank - 1,  # 0-indexed
                        'is_synthetic': True,
                        'session_id': session_id
                    }
                    
                    response = self.session.post(
                        f"{self.event_collector_url}/events/click",
                        json=click_event,
                        timeout=5
                    )
                    
                    if response.status_code == 200:
                        stats['clicks'] += 1
                    else:
                        logger.warning(f"Failed to log click: {response.status_code}")
                        stats['errors'] += 1
                        continue
                    
                    # Step 3: Decide if user watches
                    if random.random() < persona['watch_prob']:
                        # Generate movie duration
                        duration_category = random.choices(
                            ['short', 'medium', 'long'],
                            weights=[0.3, 0.5, 0.2]
                        )[0]
                        total_duration = random.randint(*MOVIE_DURATIONS[duration_category]) * 60  # seconds
                        
                        # Generate watch duration
                        watch_duration = self.generate_watch_duration(
                            total_duration,
                            persona['completion_mean']
                        )
                        
                        watch_event = {
                            'event_type': 'watch',
                            'user_id': user_id,
                            'item_id': item_id,
                            'watch_duration_seconds': watch_duration,
                            'total_duration_seconds': total_duration,
                            'recommendation_id': recommendation_id,
                            'is_synthetic': True,
                            'session_id': session_id
                        }
                        
                        response = self.session.post(
                            f"{self.event_collector_url}/events/watch",
                            json=watch_event,
                            timeout=5
                        )
                        
                        if response.status_code == 200:
                            stats['watches'] += 1
                        else:
                            logger.warning(f"Failed to log watch: {response.status_code}")
                            stats['errors'] += 1
                            continue
                        
                        # Step 4: Decide if user gives feedback
                        completion_rate = watch_duration / total_duration
                        feedback_type = self.decide_feedback(completion_rate, persona)
                        
                        if feedback_type:
                            # Generate rating if feedback_type is 'rating'
                            rating = None
                            if feedback_type == 'rating':
                                if completion_rate > 0.7:
                                    rating = random.uniform(4.0, 5.0)
                                elif completion_rate > 0.4:
                                    rating = random.uniform(3.0, 4.0)
                                else:
                                    rating = random.uniform(1.0, 3.0)
                            
                            feedback_event = {
                                'event_type': 'feedback',
                                'user_id': user_id,
                                'item_id': item_id,
                                'feedback_type': feedback_type,
                                'rating': rating,
                                'recommendation_id': recommendation_id,
                                'is_synthetic': True,
                                'session_id': session_id
                            }
                            
                            response = self.session.post(
                                f"{self.event_collector_url}/events/feedback",
                                json=feedback_event,
                                timeout=5
                            )
                            
                            if response.status_code == 200:
                                stats['feedback'] += 1
                            else:
                                logger.warning(f"Failed to log feedback: {response.status_code}")
                                stats['errors'] += 1
                    
                    # Users typically click 1-2 items per session
                    if stats['clicks'] >= random.randint(1, 3):
                        break
            
            return stats
            
        except Exception as e:
            logger.error(f"Session simulation failed: {e}", exc_info=True)
            stats['errors'] += 1
            return stats

# ============================================================
# MAIN SIMULATION
# ============================================================

def run_simulation(
    n_users: int = 100,
    n_sessions: int = 1000,
    delay_ms: int = 100,
    api_url: str = API_URL,
    event_collector_url: str = EVENT_COLLECTOR_URL
):
    """
    Run user behavior simulation
    
    Args:
        n_users: Number of unique users to simulate
        n_sessions: Total number of sessions to generate
        delay_ms: Delay between sessions in milliseconds
        api_url: API service URL
        event_collector_url: Event collector URL
    """
    logger.info("=" * 80)
    logger.info("STARTING USER BEHAVIOR SIMULATION")
    logger.info("=" * 80)
    logger.info(f"Users: {n_users}")
    logger.info(f"Sessions: {n_sessions}")
    logger.info(f"API: {api_url}")
    logger.info(f"Event Collector: {event_collector_url}")
    logger.info("")
    
    # Check services are available
    try:
        r = requests.get(f"{api_url}/health", timeout=5)
        logger.info(f"✓ API Service: {r.json().get('status')}")
    except Exception as e:
        logger.error(f"✗ API Service unavailable: {e}")
        return
    
    try:
        r = requests.get(f"{event_collector_url}/health", timeout=5)
        logger.info(f"✓ Event Collector: {r.json().get('status')}")
    except Exception as e:
        logger.error(f"✗ Event Collector unavailable: {e}")
        return
    
    logger.info("")
    logger.info("Starting simulation...")
    logger.info("")
    
    simulator = UserBehaviorSimulator(api_url, event_collector_url)
    
    total_stats = {
        'impressions': 0,
        'clicks': 0,
        'watches': 0,
        'feedback': 0,
        'errors': 0
    }
    
    start_time = time.time()
    
    for session_num in range(1, n_sessions + 1):
        # Select a user (with repeat users)
        user_id = random.randint(1, n_users)
        
        # Assign persona
        persona = simulator.assign_persona()
        
        # Generate session ID
        session_id = str(uuid.uuid4())
        
        # Simulate session
        session_stats = simulator.simulate_session(user_id, persona, session_id)
        
        # Aggregate stats
        for key in total_stats:
            total_stats[key] += session_stats[key]
        
        # Progress logging
        if session_num % 50 == 0:
            elapsed = time.time() - start_time
            rate = session_num / elapsed
            ctr = (total_stats['clicks'] / total_stats['impressions'] * 100) if total_stats['impressions'] > 0 else 0
            watch_rate = (total_stats['watches'] / total_stats['clicks'] * 100) if total_stats['clicks'] > 0 else 0
            
            logger.info(
                f"Progress: {session_num}/{n_sessions} sessions | "
                f"Rate: {rate:.1f} sess/s | "
                f"CTR: {ctr:.1f}% | "
                f"Watch Rate: {watch_rate:.1f}%"
            )
        
        # Rate limiting
        if delay_ms > 0:
            time.sleep(delay_ms / 1000.0)
    
    # Final stats
    elapsed = time.time() - start_time
    logger.info("")
    logger.info("=" * 80)
    logger.info("SIMULATION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Duration: {elapsed:.1f}s")
    logger.info(f"Sessions: {n_sessions}")
    logger.info(f"Rate: {n_sessions/elapsed:.1f} sessions/s")
    logger.info("")
    logger.info("Event Statistics:")
    logger.info(f"  Impressions: {total_stats['impressions']}")
    logger.info(f"  Clicks: {total_stats['clicks']}")
    logger.info(f"  Watches: {total_stats['watches']}")
    logger.info(f"  Feedback: {total_stats['feedback']}")
    logger.info(f"  Errors: {total_stats['errors']}")
    logger.info("")
    
    if total_stats['impressions'] > 0:
        ctr = total_stats['clicks'] / total_stats['impressions'] * 100
        logger.info(f"Click-Through Rate: {ctr:.2f}%")
    
    if total_stats['clicks'] > 0:
        watch_rate = total_stats['watches'] / total_stats['clicks'] * 100
        logger.info(f"Watch Rate: {watch_rate:.2f}%")
    
    if total_stats['watches'] > 0:
        feedback_rate = total_stats['feedback'] / total_stats['watches'] * 100
        logger.info(f"Feedback Rate: {feedback_rate:.2f}%")
    
    logger.info("")
    logger.info("All events tagged as synthetic (is_synthetic=true)")
    logger.info("=" * 80)

def main():
    parser = argparse.ArgumentParser(description="Simulate user behavior for ML feedback loop")
    parser.add_argument('--users', type=int, default=100, help='Number of unique users')
    parser.add_argument('--sessions', type=int, default=1000, help='Number of sessions to simulate')
    parser.add_argument('--delay', type=int, default=100, help='Delay between sessions (ms)')
    parser.add_argument('--api-url', default=API_URL, help='API service URL')
    parser.add_argument('--event-url', default=EVENT_COLLECTOR_URL, help='Event collector URL')
    
    args = parser.parse_args()
    
    run_simulation(
        n_users=args.users,
        n_sessions=args.sessions,
        delay_ms=args.delay,
        api_url=args.api_url,
        event_collector_url=args.event_url
    )

if __name__ == '__main__':
    main()
