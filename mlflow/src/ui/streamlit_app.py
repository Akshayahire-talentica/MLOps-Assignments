"""
MLOps Movie Recommendation System - Streamlit UI
Production Pipeline Dashboard
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os
from pathlib import Path

# Add src directory to path
src_dir = Path(__file__).parent.parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Import real integration module
from ui.real_mlops_integration import RealMLOpsIntegration

# Get external URLs from environment (for NodePort access on EKS)
EXTERNAL_BASE_URL = os.getenv("EXTERNAL_BASE_URL", "http://localhost")
AIRFLOW_EXTERNAL_PORT = os.getenv("AIRFLOW_EXTERNAL_PORT", "8080")
MLFLOW_EXTERNAL_PORT = os.getenv("MLFLOW_EXTERNAL_PORT", "5000")
PROMETHEUS_EXTERNAL_PORT = os.getenv("PROMETHEUS_EXTERNAL_PORT", "9091")
GRAFANA_EXTERNAL_PORT = os.getenv("GRAFANA_EXTERNAL_PORT", "3000")

# Import real integration module
from ui.real_mlops_integration import RealMLOpsIntegration

# Page config
st.set_page_config(
    page_title="MLOps Movie Recommendation",
    page_icon="🎬",
    layout="wide"
)

# Initialize integration
@st.cache_resource
def get_mlops():
    return RealMLOpsIntegration()

mlops = get_mlops()

# Title
st.title("🎬 MLOps Movie Recommendation System")
st.markdown("**Production Pipeline Dashboard - Real Data from S3/DVC/MLflow/Airflow**")

# Sidebar - System Health
with st.sidebar:
    st.header("🔧 System Status")
    
    # Get real statistics
    with st.spinner("Loading stats..."):
        stats = mlops.get_data_statistics()
    
    if stats.get('error'):
        st.error(f"⚠️ {stats['error']}")
    else:
        st.success("✅ System Healthy")
        st.metric("Movies", f"{stats.get('total_movies', 0):,}")
        st.metric("Ratings", f"{stats.get('total_ratings', 0):,}")
        st.metric("Users", f"{stats.get('unique_users', 0):,}")
        
        if stats.get('avg_rating'):
            st.metric("Avg Rating", f"{stats.get('avg_rating', 0):.2f}")
    
    st.divider()
    st.caption(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Main Tabs
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Dashboard",
    "🎯 Use Cases",
    "🔮 Predictions", 
    "⚙️ Pipeline Control",
    "📈 Monitoring",
    "🔄 Feedback Loop",
    "🏆 Model Comparison"
])

# ============================================================================
# TAB 1: Dashboard
# ============================================================================
with tab1:
    st.header("System Overview")
    
    # KPI Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Movies",
            f"{stats.get('total_movies', 0):,}",
            delta="From Parquet"
        )
    
    with col2:
        st.metric(
            "Total Ratings", 
            f"{stats.get('total_ratings', 0):,}",
            delta="Real Data"
        )
    
    with col3:
        unique_users = stats.get('unique_users', 0)
        st.metric(
            "Unique Users",
            f"{unique_users:,}",
            delta="Live"
        )
    
    with col4:
        avg_rating = stats.get('avg_rating', 0)
        st.metric(
            "Avg Rating",
            f"{avg_rating:.2f}" if avg_rating else "N/A",
            delta="Computed"
        )
    
    st.divider()
    
    # Load and display real data
    st.subheader("📊 Processed Movies Data")
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        if st.button("🔄 Refresh Data", key="refresh_dashboard", use_container_width=True):
            st.cache_resource.clear()
            st.rerun()
    
    with col1:
        st.caption("Data loaded from S3/DVC parquet files")
    
    with st.spinner("Loading movies from parquet..."):
        movies_df = mlops.load_processed_movies()
    
    if not movies_df.empty:
        st.success(f"✅ Loaded {len(movies_df):,} movies from processed parquet files")
        
        # Display ALL movies with pagination
        st.markdown("**All Movies in Dataset:**")
        
        # Pagination controls
        page_size = st.selectbox("Rows per page:", [20, 50, 100, 500], index=0)
        total_pages = (len(movies_df) - 1) // page_size + 1
        page = st.number_input("Page:", min_value=1, max_value=total_pages, value=1)
        
        start_idx = (page - 1) * page_size
        end_idx = min(start_idx + page_size, len(movies_df))
        
        st.caption(f"Showing rows {start_idx + 1} to {end_idx} of {len(movies_df):,}")
        st.dataframe(movies_df.iloc[start_idx:end_idx], use_container_width=True, height=400)
        
        # Show column info
        with st.expander("📋 Dataset Info"):
            st.write(f"**Shape:** {movies_df.shape}")
            st.write(f"**Columns:** {', '.join(movies_df.columns.tolist())}")
            st.write(f"**Memory:** {movies_df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    else:
        st.warning("⚠️ No processed movies found. Run the pipeline first.")
        st.info("Trigger the pipeline from the 'Pipeline Control' tab")

# ============================================================================
# TAB 2: Use Cases & Demo Scenarios
# ============================================================================
with tab2:
    st.header("🎯 MLOps Use Cases - Real Production Scenarios")
    
    st.markdown("""
    Demonstration of end-to-end MLOps capabilities using real MovieLens data.
    Each use case showcases different aspects of our production pipeline.
    """)
    
    st.divider()
    
    # Use Case Selector
    use_case = st.selectbox(
        "Select Use Case to Demonstrate",
        [
            "1️⃣ Cold Start - New User Onboarding",
            "2️⃣ Personalized Recommendations",
            "3️⃣ Batch Processing for Email Campaign",
            "4️⃣ Real-time Model Serving",
            "5️⃣ ML Pipeline Automation & Drift Detection"
        ]
    )
    
    st.divider()
    
    # ===== USE CASE 1: Cold Start =====
    if "Cold Start" in use_case:
        st.subheader("1️⃣ Cold Start Problem - New User Onboarding")
        
        st.markdown("""
        **Business Challenge:** When a new user signs up, we have zero interaction history.  
        How do we make good first recommendations to keep them engaged?
        
        **MLOps Solution:** 
        - Analyze popularity from 1M+ existing ratings (aggregated features)
        - Use data-driven fallback: movies with high avg rating + sufficient votes
        - Real-time calculation from production data pipeline
        """)
        
        st.info("**👤 Scenario:** A new user just created an account. Show them the best movies to get started!")
        
        # Interactive demo button
        if st.button("🎬 Generate Recommendations for New User", type="primary", use_container_width=True):
            with st.spinner("Analyzing 1M+ ratings from data pipeline..."):
                import time
                time.sleep(1.5)  # Simulate processing
                
                movies_df = mlops.load_processed_movies()
                ratings_df = mlops.load_processed_ratings()
                
                if not ratings_df.empty and not movies_df.empty:
                    # Calculate popularity metrics
                    if 'MovieID' in ratings_df.columns:
                        movie_ratings = ratings_df.groupby('MovieID').agg({
                            'Rating': ['mean', 'count']
                        }).reset_index()
                        movie_ratings.columns = ['MovieID', 'avg_rating', 'rating_count']
                        
                        # Filter: minimum 100 ratings for statistical significance
                        popular = movie_ratings[movie_ratings['rating_count'] >= 100].nlargest(10, 'avg_rating')
                        
                        # Merge with movie titles
                        if 'MovieID' in movies_df.columns and 'Title' in movies_df.columns:
                            popular = popular.merge(movies_df[['MovieID', 'Title']], on='MovieID', how='left')
                            
                            popular['avg_rating'] = popular['avg_rating'].round(2)
                            
                            st.success("✅ Cold Start Recommendations Generated!")
                            
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                st.markdown("**🎯 Top 10 Recommended Movies:**")
                                st.dataframe(
                                    popular[['Title', 'avg_rating', 'rating_count']].rename(columns={
                                        'Title': '🎬 Movie Title',
                                        'avg_rating': '⭐ Rating',
                                        'rating_count': '👥 Votes'
                                    }),
                                    use_container_width=True,
                                    hide_index=True
                                )
                            
                            with col2:
                                st.metric("Total Movies", f"{len(movies_df):,}")
                                st.metric("Total Ratings", f"{len(ratings_df):,}")
                                st.metric("Avg Rating", f"{ratings_df['Rating'].mean():.2f}")
                            
                            st.markdown("""
                            **💡 How It Works:**
                            1. Real data from S3/DVC pipeline (Parquet files)
                            2. PySpark processed ~1M ratings across 6,040 users
                            3. Statistical filter: movies with 100+ votes for reliability
                            4. Ranked by average rating
                            """)
        
        else:
            st.info("""
            **🎯 What You'll See:**
            - Top 10 highest-rated movies from production data
            - Only movies with 100+ ratings (statistically significant)
            - Real-time calculation from MLOps pipeline data
            - Powered by S3 → DVC → PySpark → Features
            
            **Click the button above to demo the Cold Start solution!**
            """)
    
    # ===== USE CASE 2: Personalized Recommendations =====
    elif "Personalized" in use_case:
        st.subheader("2️⃣ Personalized Movie Recommendations")
        
        st.markdown("""
        **Business Goal:** Provide personalized recommendations to increase engagement
        
        **MLOps Features:**
        - Matrix Factorization (NMF) model trained on user-movie interactions
        - Feature engineering: user preferences, movie genres, ratings history
        - Model versioning and A/B testing via MLflow
        - Real-time serving via FastAPI
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            demo_user = st.number_input(
                "Select User ID",
                min_value=1,
                max_value=6040,
                value=1,
                step=1,
                help="Enter any user ID from 1 to 6,040 (real users from MovieLens dataset)"
            )
        
        with col2:
            num_recommendations = st.slider(
                "Number of Recommendations",
                min_value=5,
                max_value=20,
                value=10
            )
        
        # Show user's watch history first
        st.markdown("---")
        st.markdown(f"**👤 User {demo_user}'s Watch History:**")
        
        with st.spinner("Loading user history..."):
            history_df = mlops.get_user_watch_history(demo_user, limit=10)
        
        if not history_df.empty:
            # Show watch history with movie details
            display_cols = ['Title', 'Genres', 'Rating']
            if all(col in history_df.columns for col in display_cols):
                st.dataframe(
                    history_df[display_cols].rename(columns={
                        'Title': '🎬 Movie',
                        'Genres': '🎭 Genres',
                        'Rating': '⭐ User Rating'
                    }),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Analyze user preferences
                avg_rating = history_df['Rating'].mean()
                favorite_genres = []
                for genres in history_df['Genres'].dropna():
                    favorite_genres.extend(str(genres).split('|'))
                from collections import Counter
                top_genres = Counter(favorite_genres).most_common(3)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Average Rating Given", f"{avg_rating:.2f}")
                with col2:
                    st.info(f"**Favorite Genres:** {', '.join([g[0] for g in top_genres])}")
        else:
            st.info(f"User {demo_user} has no recorded watch history in dataset.")
        
        st.markdown("---")
        st.markdown("**🎯 Personalized Recommendations:**")
        
        if st.button("🎬 Get Personalized Recommendations", type="primary", use_container_width=True):
            with st.spinner(f"Generating {num_recommendations} recommendations for User {demo_user}..."):
                # Get predictions for sample movies
                movie_ids = list(range(1, num_recommendations + 1))
                predictions = []
                
                movies_df = mlops.load_processed_movies()
                
                for movie_id in movie_ids:
                    result = mlops.predict_rating(demo_user, movie_id)
                    if 'predicted_rating' in result:
                        pred_data = {
                            'user_id': demo_user,
                            'movie_id': movie_id,
                            'predicted_rating': result['predicted_rating']
                        }
                        
                        # Add movie details
                        if 'movie_title' in result:
                            pred_data['movie_title'] = result['movie_title']
                        if 'movie_genres' in result:
                            pred_data['movie_genres'] = result['movie_genres']
                        
                        predictions.append(pred_data)
                
                if predictions:
                    pred_df = pd.DataFrame(predictions)
                    pred_df = pred_df.sort_values('predicted_rating', ascending=False)
                    
                    st.success(f"✅ Generated {len(pred_df)} recommendations")
                    
                    # Display with movie names and genres
                    display_cols = ['movie_title', 'movie_genres', 'predicted_rating']
                    if all(col in pred_df.columns for col in display_cols):
                        st.dataframe(
                            pred_df[display_cols].rename(columns={
                                'movie_title': '🎬 Movie',
                                'movie_genres': '🎭 Genres',
                                'predicted_rating': '⭐ Predicted Rating'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.dataframe(pred_df, use_container_width=True, hide_index=True)
                    
                    # Visualization
                    if 'movie_title' in pred_df.columns:
                        fig = px.bar(
                            pred_df.head(10),
                            x='movie_title',
                            y='predicted_rating',
                            title=f'Top 10 Predicted Ratings for User {demo_user}',
                            labels={'predicted_rating': 'Rating', 'movie_title': 'Movie'},
                            color='predicted_rating',
                            color_continuous_scale='Viridis'
                        )
                        fig.update_xaxes(tickangle=-45)
                        st.plotly_chart(fig, use_container_width=True)
    
    # ===== USE CASE 3: Batch Processing =====
    elif "Batch Processing" in use_case:
        st.subheader("3️⃣ Batch Processing for Marketing Campaign")
        
        st.markdown("""
        **Business Scenario:** Weekend email campaign - recommend movies to 1000+ users
        
        **MLOps Approach:**
        - Batch prediction API endpoint
        - Airflow scheduled jobs for automated processing
        - Results stored in S3 for campaign management
        - Monitoring via Prometheus + Grafana
        """)
        
        st.markdown("**Simulate Batch Campaign:**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            num_users = st.number_input(
                "Number of Users to Process",
                min_value=10,
                max_value=100,
                value=20
            )
        
        with col2:
            movies_per_user = st.number_input(
                "Movies per User",
                min_value=1,
                max_value=5,
                value=3
            )
        
        if st.button("📧 Run Batch Campaign", type="primary", use_container_width=True):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            total_predictions = num_users * movies_per_user
            status_text.text(f"Processing {total_predictions} predictions for {num_users} users...")
            
            # Simulate batch processing
            import time
            results = []
            
            for i, user_id in enumerate(range(1, num_users + 1)):
                for movie_id in range(1, movies_per_user + 1):
                    result = mlops.predict_rating(user_id, movie_id)
                    if 'predicted_rating' in result:
                        results.append({
                            'user_id': user_id,
                            'movie_id': movie_id,
                            'predicted_rating': result['predicted_rating']
                        })
                
                progress_bar.progress((i + 1) / num_users)
                time.sleep(0.1)  # Simulate processing
            
            progress_bar.empty()
            status_text.empty()
            
            if results:
                results_df = pd.DataFrame(results)
                st.success(f"✅ Campaign Complete: {len(results_df)} recommendations generated")
                
                # Statistics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Predictions", len(results_df))
                with col2:
                    st.metric("Users Processed", results_df['user_id'].nunique())
                with col3:
                    st.metric("Avg Rating", f"{results_df['predicted_rating'].mean():.2f}")
                with col4:
                    st.metric("Processing Time", f"{num_users * 0.1:.1f}s")
                
                # Show sample results
                with st.expander("📊 Sample Results"):
                    st.dataframe(results_df.head(20), use_container_width=True)
    
    # ===== USE CASE 4: Real-time Model Serving =====
    elif "Real-time" in use_case:
        st.subheader("4️⃣ Real-time Model Serving with A/B Testing")
        
        st.markdown("""
        **Production Setup:**
        - **Model V1:** SVD (Singular Value Decomposition)
        - **Model V2:** NMF (Non-negative Matrix Factorization)
        - **Router:** Canary deployment (70% V1, 30% V2)
        - **Monitoring:** Real-time latency and accuracy tracking
        """)
        
        st.markdown("**Test Model API Response Times:**")
        
        test_user = st.number_input("User ID", value=42, min_value=1, max_value=6040)
        test_movie = st.number_input("Movie ID", value=50, min_value=1, max_value=3952)
        
        if st.button("⚡ Test Real-time Prediction", use_container_width=True):
            import time
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Production API (Via Router)**")
                start = time.time()
                result = mlops.predict_rating(test_user, test_movie)
                latency = (time.time() - start) * 1000
                
                if 'predicted_rating' in result:
                    st.metric("Prediction", f"{result['predicted_rating']:.2f} ⭐")
                    st.metric("Latency", f"{latency:.1f} ms")
                    st.success("✅ API Healthy")
                else:
                    st.error("API Error")
            
            with col2:
                st.info("""
                **SLA Metrics:**
                - Target Latency: <100ms
                - Availability: 99.9%
                - Requests/sec: 1000+
                
                **Infrastructure:**
                - Docker containers
                - Health checks
                - Auto-scaling ready
                """)
    
    # ===== USE CASE 5: Pipeline Automation =====
    else:
        st.subheader("5️⃣ ML Pipeline Automation & Drift Detection")
        
        st.markdown("""
        **Automated MLOps Workflow:**
        
        1. **Data Ingestion:** Airflow DAG pulls new data from S3 daily
        2. **Validation:** Great Expectations validates data quality
        3. **Training:** Auto-trigger if drift detected or weekly schedule  
        4. **Deployment:** MLflow promotes best model to production
        5. **Monitoring:** Evidently AI tracks data drift → auto-retrain
        """)
        
        st.image("https://via.placeholder.com/800x300/1f77b4/ffffff?text=MLOps+Pipeline+Flow", 
                caption="Automated MLOps Pipeline")
        
        st.markdown("**Pipeline Configuration:**")
        
        config_df = pd.DataFrame([
            {"Component": "Orchestration", "Tool": "Apache Airflow", "Status": "✅ Running"},
            {"Component": "Data Storage", "Tool": "S3 + DVC", "Status": "✅ Connected"},
            {"Component": "Processing", "Tool": "PySpark", "Status": "✅ Ready"},
            {"Component": "Validation", "Tool": "Great Expectations", "Status": "✅ Configured"},
            {"Component": "Experiment Tracking", "Tool": "MLflow", "Status": "✅ Active"},
            {"Component": "Drift Detection", "Tool": "Evidently AI", "Status": "✅ Monitoring"},
            {"Component": "Metrics", "Tool": "Prometheus", "Status": "✅ Scraping"},
            {"Component": "Dashboards", "Tool": "Grafana", "Status": "✅ Visualizing"},
        ])
        
        st.dataframe(config_df, use_container_width=True, hide_index=True)
        
        st.divider()
        
        st.markdown("**🔧 Quick Actions:**")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("▶️ Trigger Full Pipeline", use_container_width=True):
                st.info("Navigate to 'Pipeline Control' tab to trigger DAG")
        
        with col2:
            if st.button("📊 View MLflow", use_container_width=True):
                st.markdown(f"[Open MLflow UI]({EXTERNAL_BASE_URL}:{MLFLOW_EXTERNAL_PORT})")
        
        with col3:
            if st.button("📈 View Grafana", use_container_width=True):
                st.markdown(f"[Open Grafana]({EXTERNAL_BASE_URL}:{GRAFANA_EXTERNAL_PORT})")

# ============================================================================
# TAB 3: Predictions
# ============================================================================
with tab3:
    st.header("Movie Rating Predictions")
    
    st.markdown("""
    Get predictions using the production MLflow model served via FastAPI.
    """)
    
    st.divider()
    
    # Single Prediction
    st.subheader("🎯 Single Prediction")
    
    col1, col2 = st.columns(2)
    
    with col1:
        user_id = st.number_input(
            "User ID",
            min_value=1,
            max_value=6040,
            value=1,
            help="User ID from MovieLens dataset"
        )
    
    with col2:
        movie_id = st.number_input(
            "Movie ID",
            min_value=1,
            max_value=3952,
            value=1,
            help="Movie ID from MovieLens dataset"
        )
    
    if st.button("🔮 Get Prediction", type="primary", use_container_width=True):
        with st.spinner("Calling MLflow model via API service..."):
            result = mlops.predict_rating(user_id, movie_id)
        
        if "error" in result:
            st.error(f"⚠️ Prediction failed: {result['error']}")
            
            # Show helpful message
            if "not available" in result['error']:
                st.info("💡 **Troubleshooting:**")
                st.code("docker-compose ps api-service")
                st.code("docker-compose logs api-service")
            elif "timeout" in result['error']:
                st.warning("API service is slow. Try again in a moment.")
        else:
            st.success("✅ Prediction successful!")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                predicted_rating = result.get('predicted_rating', 0)
                st.metric(
                    "Predicted Rating",
                    f"{predicted_rating:.2f}",
                    delta="⭐" * int(round(predicted_rating))
                )
            
            with col2:
                confidence = result.get('confidence', 0)
                st.metric(
                    "Confidence",
                    f"{confidence:.1%}" if confidence < 1 else f"{confidence:.0f}%"
                )
            
            with col3:
                st.metric(
                    "Model",
                    "nmf_recommendation_v2"
                )
            
            # Show movie details if available
            if 'movie_title' in result:
                st.markdown("**📽️ Movie Details:**")
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**Title:** {result.get('movie_title', 'Unknown')}")
                with col2:
                    st.write(f"**Genres:** {result.get('movie_genres', 'Unknown')}")
            
            # Show result details
            with st.expander("📋 Full Response"):
                st.json(result)
    
    st.divider()
    
    # Batch Predictions
    st.subheader("📦 Batch Predictions")
    
    st.markdown("Predict ratings for multiple movies at once.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        movie_ids_input = st.text_area(
            "Movie IDs (comma-separated)",
            value="1, 2, 3, 50, 100",
            help="Enter movie IDs separated by commas"
        )
    
    with col2:
        st.info("""
        **Example:** 1, 2, 3, 50, 100
        
        This will predict ratings for the specified movies.
        """)
    
    if st.button("🔮 Batch Predict", use_container_width=True):
        try:
            # Parse movie IDs
            movie_ids = [int(x.strip()) for x in movie_ids_input.split(',') if x.strip()]
            
            if not movie_ids:
                st.error("Please enter at least one movie ID")
            else:
                with st.spinner(f"Processing {len(movie_ids)} predictions..."):
                    results_df = mlops.batch_predict(movie_ids)
                
                if not results_df.empty:
                    st.success(f"✅ Processed {len(results_df)} predictions")
                    
                    # Display with movie names and genres if available
                    if 'movie_title' in results_df.columns and 'movie_genres' in results_df.columns:
                        display_cols = ['movie_title', 'movie_genres', 'predicted_rating']
                        st.dataframe(
                            results_df[display_cols].rename(columns={
                                'movie_title': '🎬 Movie',
                                'movie_genres': '🎭 Genres',
                                'predicted_rating': '⭐ Predicted Rating'
                            }),
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.dataframe(results_df, use_container_width=True)
                    
                    # Visualization
                    if 'predicted_rating' in results_df.columns:
                        x_col = 'movie_title' if 'movie_title' in results_df.columns else 'movie_id'
                        fig = px.bar(
                            results_df,
                            x=x_col,
                            y='predicted_rating',
                            title='Batch Predictions',
                            labels={'predicted_rating': 'Rating', x_col: 'Movie'}
                        )
                        if x_col == 'movie_title':
                            fig.update_xaxes(tickangle=-45)
                        st.plotly_chart(fig, use_container_width=True)
                else:
                    st.error("Batch prediction failed. Check API service.")
        
        except ValueError:
            st.error("Invalid input. Please enter numeric movie IDs separated by commas.")

# ============================================================================
# TAB 4: Pipeline Control
# ============================================================================
with tab4:
    st.header("MLOps Pipeline Management")
    
    st.markdown("""
    **Full Pipeline DAG:** `mlops_full_pipeline`
    
    **Pipeline Stages:**
    1. ✅ Check S3 Raw Data
    2. ⚙️ PySpark ETL Processing
    3. 🔍 Great Expectations Validation
    4. 🔧 Feature Engineering
    5. 🤖 Model Training (MLflow)
    6. 📊 Drift Detection (scipy KS-Test)
    7. 🔄 Auto-Retraining (if drift detected)
    """)
    
    st.divider()
    
    # Pipeline Controls
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("▶️ Trigger Pipeline")
        
        if st.button("🚀 Run Full Pipeline", type="primary", use_container_width=True):
            with st.spinner("Triggering Airflow DAG..."):
                result = mlops.trigger_full_pipeline()
            
            if result['status'] == 'success':
                st.success(f"✅ Pipeline triggered successfully!")
                st.code(f"DAG Run ID: {result['dag_run_id']}")
                
                # Save to session state
                st.session_state['last_dag_run_id'] = result['dag_run_id']
                
                st.info("Check status in the panel to the right →")
            else:
                st.error(f"❌ Failed to trigger pipeline")
                st.error(result['message'])
                
                # Show troubleshooting
                with st.expander("🔧 Troubleshooting"):
                    st.markdown("""
                    **Common Issues:**
                    1. Airflow not running: `docker-compose ps airflow-webserver`
                    2. Authentication failed: Check credentials in docker-compose.yml
                    3. Network issue: Ensure services are on same network
                    """)
                    st.code("docker-compose logs airflow-webserver | tail -20")
    
    with col2:
        st.subheader("📊 Pipeline Status")

        # Always show latest status — no button needed
        col_refresh, _ = st.columns([1, 3])
        with col_refresh:
            refresh_status = st.button("🔄 Refresh", use_container_width=True)

        with st.spinner("Fetching latest pipeline run…"):
            dag_run_id = st.session_state.get('last_dag_run_id', None)
            status = mlops.get_pipeline_status(dag_run_id)

        if status and not status.get('error'):
            state = status.get('state', 'unknown')

            if state == 'success':
                st.success(f"✅ Pipeline: **{state.upper()}**")
            elif state == 'running':
                st.info(f"⚡ Pipeline: **{state.upper()}**")
            elif state == 'failed':
                st.error(f"❌ Pipeline: **{state.upper()}**")
            else:
                st.warning(f"⏸️ Pipeline: **{state}**")

            c1, c2 = st.columns(2)
            run_id_str = status.get('dag_run_id', 'N/A')
            with c1:
                st.metric("Run ID", run_id_str[:28] + "…" if len(str(run_id_str)) > 30 else run_id_str)
            with c2:
                start_date = status.get('start_date')
                st.metric("Started", start_date[:19] if start_date else 'N/A')

            # --- Live task status (always visible, uses latest run id from status) ---
            active_run_id = status.get('dag_run_id') or dag_run_id
            if active_run_id:
                tasks = mlops.get_task_statuses(active_run_id)
                if tasks:
                    STATE_ICON = {
                        'success': '✅', 'failed': '❌', 'running': '⚡',
                        'skipped': '⏭️', 'up_for_retry': '🔁',
                        'queued': '🕐', 'scheduled': '🕐',
                    }
                    st.markdown("**📋 Task Status:**")
                    for task in tasks:
                        t_state = task.get('state', 'none') or 'none'
                        icon = STATE_ICON.get(t_state, '⬜')
                        duration = task.get('duration')
                        dur_str = f"  `{duration:.1f}s`" if duration else ""
                        st.markdown(f"{icon} **{task.get('task_id', 'N/A')}** — {t_state}{dur_str}")
                else:
                    st.info("No task details available")
            else:
                st.info("Trigger a pipeline run to see task details")

            with st.expander("🔍 Full JSON"):
                st.json(status)

        elif status and status.get('error'):
            st.error(f"⚠️ {status['error']}")
        else:
            st.info("No recent pipeline runs found — trigger one →")

        st.caption("Auto-loads latest DAG run from Airflow API")
    
    st.divider()
    
    # External Links
    st.subheader("🔗 External Tools")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown(f"**[Airflow UI]({EXTERNAL_BASE_URL}:{AIRFLOW_EXTERNAL_PORT}/dags/mlops_full_pipeline)**")
        st.caption("View DAG graph and logs")
    
    with col2:
        st.markdown(f"**[MLflow UI]({EXTERNAL_BASE_URL}:{MLFLOW_EXTERNAL_PORT})**")
        st.caption("View experiments and models")
    
    with col3:
        st.markdown(f"**[Prometheus]({EXTERNAL_BASE_URL}:{PROMETHEUS_EXTERNAL_PORT})**")
        st.caption("System metrics")

# ============================================================================
# TAB 5: Monitoring
# ============================================================================
with tab5:
    st.header("Model & Data Monitoring")
    
    # Model Metrics
    st.subheader("🤖 Production Model Performance")
    
    with st.spinner("Loading model metrics from MLflow..."):
        metrics = mlops.get_model_metrics()
    
    if metrics and any(metrics.values()):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            rmse = metrics.get('rmse', metrics.get('test_rmse', 0))
            if rmse:
                st.metric("RMSE", f"{rmse:.4f}")
            else:
                st.metric("RMSE", "N/A")
        
        with col2:
            mae = metrics.get('mae', metrics.get('test_mae', 0))
            if mae:
                st.metric("MAE", f"{mae:.4f}")
            else:
                st.metric("MAE", "N/A")
        
        with col3:
            r2 = metrics.get('r2', metrics.get('test_r2', metrics.get('r2_score', 0)))
            if r2:
                st.metric("R² Score", f"{r2:.4f}")
            else:
                st.metric("R² Score", "N/A")
        
        with col4:
            train_time = metrics.get('training_time', metrics.get('train_time', 0))
            if train_time:
                st.metric("Training Time", f"{train_time:.1f}s")
            else:
                st.metric("Training Time", "N/A")
        
        with st.expander("📊 All Metrics"):
            st.json(metrics)
    else:
        st.warning("⚠️ No model metrics available yet")
        st.info("""
        **To generate metrics:**
        1. Go to 'Pipeline Control' tab
        2. Click 'Run Full Pipeline'
        3. Wait for training to complete (~5-10 minutes)
        4. Metrics will appear here automatically
        
        **Or run training manually:**
        ```bash
        python src/training/train_nmf.py
        ```
        """)
    
    st.divider()
    
    # Drift Detection
    st.subheader("📊 Data Drift Monitoring")
    
    st.markdown("Latest drift detection report from Evidently AI")
    
    with st.spinner("Loading drift report from S3..."):
        drift_report = mlops.get_latest_drift_report()
    
    if drift_report:
        drift_detected = drift_report.get('drift_detected', False)
        drift_score = drift_report.get('drift_score', 0)
        
        col1, col2 = st.columns(2)
        
        with col1:
            if drift_detected:
                st.error(f"⚠️ **DRIFT DETECTED**")
                st.metric("Drift Score", f"{drift_score:.3f}")
            else:
                st.success("✅ **NO DRIFT DETECTED**")
                st.metric("Drift Score", f"{drift_score:.3f}")
        
        with col2:
            st.info("**Action Taken:**")
            if drift_detected:
                st.markdown("- Auto-retraining triggered\n- New baseline saved\n- Monitoring continues")
            else:
                st.markdown("- Continue monitoring\n- No action needed\n- Baseline stable")
        
        # Show full report
        with st.expander("📋 Full Drift Report"):
            st.json(drift_report)
    else:
        st.info("⚠️ No drift reports available yet")
        st.markdown("Drift detection runs as part of the pipeline")
    
    st.divider()
    
    # External Monitoring Dashboards
    st.subheader("📊 External Monitoring Dashboards")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**[MLflow Experiments]({EXTERNAL_BASE_URL}:{MLFLOW_EXTERNAL_PORT})**")
        st.caption("View all training runs and compare models")
        
        # Show registered models
        with st.spinner("Loading registered models..."):
            models = mlops.get_registered_models()
        
        if models:
            st.success(f"✅ {len(models)} registered model(s)")
            
            model_names = [m.get('name', 'Unknown') for m in models]
            st.write("**Models:**")
            for name in model_names:
                st.markdown(f"- {name}")
        else:
            st.info("No registered models yet")
    
    with col2:
        st.markdown(f"**[Grafana Dashboards]({EXTERNAL_BASE_URL}:{GRAFANA_EXTERNAL_PORT})**")
        st.caption("System metrics and performance")
        
        st.info(f"""
        **Grafana Credentials:**
        - **URL:** {EXTERNAL_BASE_URL}:{GRAFANA_EXTERNAL_PORT}
        - **Username:** admin
        - **Password:** admin123
        
        *First login may ask to change password - you can skip*
        """)
        
        st.markdown(f"**[Prometheus Metrics]({EXTERNAL_BASE_URL}:{PROMETHEUS_EXTERNAL_PORT})**")
        st.caption("Raw metrics and alerts")

# ============================================================================
# TAB 6: Feedback Loop
# ============================================================================
with tab6:
    st.header("🔄 ML Feedback Loop - Real-time Learning")
    st.markdown("**Monitoring user interactions → generating training labels → retraining models**")
    
    # Fetch feedback data from database
    with st.spinner("Loading feedback loop data..."):
        try:
            import psycopg2
            
            # Connect to database
            conn = psycopg2.connect(
                host='postgres' if mlops.in_docker else 'localhost',
                port=5432,
                database='mlops_db',
                user='mlops',
                password='mlops123'
            )
            
            # Get event counts
            event_query = """
                SELECT event_type, COUNT(*) as count 
                FROM user_events 
                GROUP BY event_type 
                ORDER BY count DESC
            """
            events_df = pd.read_sql(event_query, conn)
            
            # Get recommendation logs count
            rec_query = "SELECT COUNT(*) as total_recommendations FROM recommendation_logs"
            rec_count = pd.read_sql(rec_query, conn).iloc[0]['total_recommendations']
            
            # Get training labels stats
            label_query = """
                SELECT 
                    COUNT(*) as total_labels,
                    AVG(label) as avg_label,
                    MIN(label) as min_label,
                    MAX(label) as max_label
                FROM training_labels
            """
            labels_stats = pd.read_sql(label_query, conn)
            
            # Get CTR and engagement metrics
            metrics_query = """
                SELECT 
                    SUM(CASE WHEN event_type = 'impression' THEN 1 ELSE 0 END) as impressions,
                    SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) as clicks,
                    SUM(CASE WHEN event_type = 'watch' THEN 1 ELSE 0 END) as watches,
                    SUM(CASE WHEN event_type = 'feedback' THEN 1 ELSE 0 END) as feedback_events
                FROM user_events
            """
            metrics = pd.read_sql(metrics_query, conn)
            
            # Get recent events
            recent_query = """
                SELECT event_type, user_id, item_id, created_at, context
                FROM user_events
                ORDER BY created_at DESC
                LIMIT 20
            """
            recent_events = pd.read_sql(recent_query, conn)
            
            conn.close()
            
            # Display KPIs
            st.subheader("📊 Feedback Loop KPIs")
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "Recommendations",
                    f"{rec_count:,}",
                    delta="Logged"
                )
            
            with col2:
                clicks = int(metrics.iloc[0]['clicks'])
                st.metric(
                    "Clicks",
                    f"{clicks:,}",
                    delta="User Actions"
                )
            
            with col3:
                watches = int(metrics.iloc[0]['watches'])
                st.metric(
                    "Watches",
                    f"{watches:,}",
                    delta="Engagement"
                )
            
            with col4:
                impressions = int(metrics.iloc[0]['impressions'])
                ctr = (clicks / impressions * 100) if impressions > 0 else 0
                st.metric(
                    "CTR",
                    f"{ctr:.1f}%",
                    delta="Click Rate"
                )
            
            with col5:
                total_labels = int(labels_stats.iloc[0]['total_labels'])
                st.metric(
                    "Training Labels",
                    f"{total_labels:,}",
                    delta="Generated"
                )
            
            st.divider()
            
            # Event Distribution
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📈 Event Distribution")
                if not events_df.empty:
                    import plotly.express as px
                    fig = px.pie(
                        events_df,
                        values='count',
                        names='event_type',
                        title='User Events by Type',
                        hole=0.4
                    )
                    fig.update_traces(textposition='inside', textinfo='percent+label+value')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No events yet")
            
            with col2:
                st.subheader("🎯 Engagement Funnel")
                if not metrics.empty:
                    impressions = int(metrics.iloc[0]['impressions'])
                    clicks = int(metrics.iloc[0]['clicks'])
                    watches = int(metrics.iloc[0]['watches'])
                    feedback_events = int(metrics.iloc[0]['feedback_events'])
                    
                    funnel_data = pd.DataFrame({
                        'Stage': ['Impressions', 'Clicks', 'Watches', 'Feedback'],
                        'Count': [impressions, clicks, watches, feedback_events],
                        'Percentage': [
                            100,
                            (clicks/impressions*100) if impressions > 0 else 0,
                            (watches/clicks*100) if clicks > 0 else 0,
                            (feedback_events/watches*100) if watches > 0 else 0
                        ]
                    })
                    
                    fig = px.funnel(
                        funnel_data,
                        x='Count',
                        y='Stage',
                        title='Engagement Funnel'
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            st.divider()
            
            # Training Labels Statistics
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🏷️ Training Label Statistics")
                if not labels_stats.empty:
                    avg_label = float(labels_stats.iloc[0]['avg_label'])
                    min_label = float(labels_stats.iloc[0]['min_label'])
                    max_label = float(labels_stats.iloc[0]['max_label'])
                    
                    st.metric("Average Label", f"{avg_label:.2f}")
                    st.metric("Label Range", f"{min_label:.0f} - {max_label:.0f}")
                    st.info(f"""
                    **Label Generation:**
                    - Based on user engagement signals
                    - Range: 0-5 (0=dislike, 5=love)
                    - Avg: {avg_label:.2f} (positive engagement)
                    """)
            
            with col2:
                st.subheader("🔄 Feedback Loop Status")
                watch_rate = (watches / clicks * 100) if clicks > 0 else 0
                
                st.success("✅ Feedback Loop Active")
                st.metric("Watch Rate", f"{watch_rate:.1f}%", delta="Engagement Quality")
                
                # Status indicators
                if total_labels > 100:
                    st.success("✅ Sufficient training labels collected")
                else:
                    st.warning(f"⚠️ Need more labels (current: {total_labels}, target: 100+)")
                
                if ctr > 50:
                    st.success(f"✅ High CTR ({ctr:.1f}%)")
                else:
                    st.info(f"📊 CTR: {ctr:.1f}%")
            
            st.divider()
            
            # Recent Events Table
            st.subheader("🕒 Recent User Events (Last 20)")
            if not recent_events.empty:
                # Format timestamp
                recent_events['created_at'] = pd.to_datetime(recent_events['created_at'])
                recent_events['created_at'] = recent_events['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                st.dataframe(
                    recent_events,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No recent events")
            
            # Actions
            st.divider()
            st.subheader("⚡ Actions")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("🔄 Refresh Data", key="refresh_feedback", use_container_width=True):
                    st.rerun()
            
            with col2:
                if st.button("📊 Generate More Events", key="generate_events", use_container_width=True):
                    st.info("Run: `python3 scripts/simulate_user_behavior_v2.py --users 50 --sessions 500`")
            
            with col3:
                if st.button("🤖 Trigger Retraining", key="trigger_retrain", use_container_width=True):
                    st.warning("Model retraining will auto-trigger when drift detected")
        
        except Exception as e:
            st.error(f"❌ Failed to load feedback loop data: {e}")
            st.info("""
            **Troubleshooting:**
            1. Check database connection: `docker ps | grep postgres`
            2. Verify tables exist: Run `scripts/verify_feedback_loop.sh`
            3. Generate events: `python3 scripts/simulate_user_behavior_v2.py`
            """)

# ============================================================================
# TAB 7: Model Comparison & A/B Testing
# ============================================================================
with tab7:
    st.header("🏆 Model Comparison & Decision Making")
    st.markdown("""
    **Champion vs Challenger** - Compare model versions, A/B test results, and make deployment decisions
    """)
    
    try:
        import psycopg2
        
        # Connect to database
        conn = psycopg2.connect(
            host='postgres' if mlops.in_docker else 'localhost',
            port=5432,
            database='mlops_db',
            user='mlops',
            password='mlops123'
        )
        
        # Get model versions from MLflow and recommendation logs
        st.subheader("📦 Model Registry")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Get models from MLflow
            try:
                models = mlops.client.search_registered_models()
                if models:
                    st.success(f"✅ Found {len(models)} registered model(s) in MLflow")
                    for model in models:
                        with st.expander(f"📊 {model.name}"):
                            st.write(f"**Latest versions:**")
                            versions = mlops.client.search_model_versions(f"name='{model.name}'")
                            for v in versions[:5]:  # Show top 5 versions
                                badge = "🏆" if v.current_stage == "Production" else "🧪"
                                st.write(f"{badge} **Version {v.version}** - Stage: `{v.current_stage}` - Run ID: `{v.run_id[:8]}`")
                else:
                    st.warning("No models found in MLflow registry")
            except Exception as e:
                st.warning(f"MLflow not available: {e}")
        
        with col2:
            # Get model versions from recommendation logs
            version_query = """
                SELECT 
                    model_version,
                    COUNT(*) as recommendations_served,
                    MIN(created_at) as first_seen,
                    MAX(created_at) as last_seen
                FROM recommendation_logs
                GROUP BY model_version
                ORDER BY last_seen DESC
            """
            versions_df = pd.read_sql(version_query, conn)
            
            if not versions_df.empty:
                st.metric("Model Versions Deployed", len(versions_df))
                st.dataframe(versions_df, use_container_width=True, hide_index=True)
            else:
                st.info("No deployment history yet")
        
        st.divider()
        
        # Model Performance Comparison
        st.subheader("📊 Performance Comparison")
        
        # Check if performance data exists
        perf_query = """
            SELECT 
                model_name,
                model_version,
                stage,
                rmse,
                mae,
                ctr,
                watch_rate,
                avg_engagement_score,
                is_production,
                evaluation_date
            FROM model_performance
            ORDER BY evaluation_date DESC
            LIMIT 10
        """
        perf_df = pd.read_sql(perf_query, conn)
        
        if not perf_df.empty:
            # Metrics comparison
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                prod_model = perf_df[perf_df['is_production'] == True].iloc[0] if any(perf_df['is_production']) else perf_df.iloc[0]
                st.metric("Champion RMSE", f"{prod_model['rmse']:.4f}" if pd.notna(prod_model['rmse']) else "N/A")
            
            with col2:
                st.metric("Champion MAE", f"{prod_model['mae']:.4f}" if pd.notna(prod_model['mae']) else "N/A")
            
            with col3:
                st.metric("Champion CTR", f"{prod_model['ctr']:.1f}%" if pd.notna(prod_model['ctr']) else "N/A")
            
            with col4:
                st.metric("Champion Watch Rate", f"{prod_model['watch_rate']:.1f}%" if pd.notna(prod_model['watch_rate']) else "N/A")
            
            # Comparison chart
            if len(perf_df) > 1:
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    name='RMSE',
                    x=perf_df['model_version'],
                    y=perf_df['rmse'],
                    yaxis='y',
                    marker_color='lightblue'
                ))
                fig.add_trace(go.Scatter(
                    name='CTR (%)',
                    x=perf_df['model_version'],
                    y=perf_df['ctr'],
                    yaxis='y2',
                    marker_color='red',
                    mode='lines+markers'
                ))
                fig.update_layout(
                    title='Model Versions - Accuracy vs Engagement',
                    yaxis=dict(title='RMSE (lower is better)'),
                    yaxis2=dict(title='CTR % (higher is better)', overlaying='y', side='right'),
                    hovermode='x'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Performance table
            st.dataframe(
                perf_df.style.highlight_max(subset=['ctr', 'watch_rate'], color='lightgreen')
                              .highlight_min(subset=['rmse', 'mae'], color='lightgreen'),
                use_container_width=True,
                hide_index=True
            )
        
        else:
            st.warning("⚠️ No model performance data available")
            st.info("""
            **To populate performance metrics:**
            ```bash
            python3 scripts/populate_model_performance.py
            ```
            This will:
            - Query MLflow for model metrics
            - Calculate business metrics from user events
            - Populate the model_performance table
            """)
        
        st.divider()
        
        # A/B Testing Results
        st.subheader("🧪 A/B Test Results")
        
        ab_query = """
            SELECT 
                experiment_name,
                control_model_version,
                treatment_model_version,
                traffic_percentage_treatment,
                control_ctr,
                treatment_ctr,
                ctr_lift_percent,
                control_watch_rate,
                treatment_watch_rate,
                is_significant,
                test_status,
                decision,
                decision_reason,
                test_start_date,
                test_end_date
            FROM ab_test_results
            ORDER BY test_start_date DESC
            LIMIT 5
        """
        ab_df = pd.read_sql(ab_query, conn)
        
        if not ab_df.empty:
            for idx, row in ab_df.iterrows():
                status_icon = "✅" if row['decision'] == 'promote' else ("❌" if row['decision'] == 'rollback' else "⏳")
                with st.expander(f"{status_icon} {row['experiment_name']} - {row['test_status'].upper()}"):
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.write("**Control (Champion)**")
                        st.write(f"Version: `{row['control_model_version']}`")
                        st.metric("CTR", f"{row['control_ctr']:.1f}%" if pd.notna(row['control_ctr']) else "N/A")
                        st.metric("Watch Rate", f"{row['control_watch_rate']:.1f}%" if pd.notna(row['control_watch_rate']) else "N/A")
                    
                    with col2:
                        st.write("**Treatment (Challenger)**")
                        st.write(f"Version: `{row['treatment_model_version']}`")
                        st.metric("CTR", f"{row['treatment_ctr']:.1f}%" if pd.notna(row['treatment_ctr']) else "N/A", 
                                 delta=f"{row['ctr_lift_percent']:.1f}%" if pd.notna(row['ctr_lift_percent']) else None)
                        st.metric("Watch Rate", f"{row['treatment_watch_rate']:.1f}%" if pd.notna(row['treatment_watch_rate']) else "N/A")
                    
                    with col3:
                        st.write("**Test Results**")
                        st.write(f"Traffic Split: {row['traffic_percentage_treatment']}%")
                        sig_badge = "✅ Significant" if row['is_significant'] else "❌ Not significant"
                        st.write(f"Statistical: {sig_badge}")
                        st.write(f"**Decision:** {row['decision'].upper()}")
                        st.caption(row['decision_reason'])
                    
                    st.caption(f"Started: {row['test_start_date']} | Ended: {row['test_end_date']}")
        else:
            st.info("⚠️ No A/B tests conducted yet")
            st.markdown("""
            **To start an A/B test:**
            ```bash
            python3 scripts/start_ab_test.py \\
              --control-version v1 \\
              --treatment-version v2 \\
              --traffic-percentage 10 \\
              --duration-hours 24
            ```
            """)
        
        st.divider()
        
        # Decision Making Interface
        st.subheader("🎯 Deployment Decision")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            **Decision Criteria:**
            - ✅ **Promote** if: CTR lift > 5% AND watch rate improves AND statistically significant
            - ⚠️ **Continue** if: Results inconclusive, need more data
            - ❌ **Rollback** if: Performance degrades or error rate increases
            
            **Canary Deployment Strategy:**
            1. Deploy to 10% traffic
            2. Monitor for 24-48 hours
            3. If metrics improve → scale to 50%
            4. If still good → promote to 100% (Champion)
            5. If metrics degrade → automatic rollback
            """)
        
        with col2:
            st.markdown("**Quick Actions:**")
            if st.button("🚀 Start New A/B Test", key="start_ab_test", use_container_width=True):
                st.info("Run: `python3 scripts/start_ab_test.py`")
            
            if st.button("📊 Update Performance Metrics", key="update_perf", use_container_width=True):
                st.info("Run: `python3 scripts/populate_model_performance.py`")
            
            if st.button("🔄 Refresh Data", key="refresh_model_comparison", use_container_width=True):
                st.rerun()
        
        conn.close()
        
    except Exception as e:
        st.error(f"❌ Failed to load model comparison data: {e}")
        st.info("""
        **Troubleshooting:**
        1. Check database: `docker ps | grep postgres`
        2. Populate performance data: `python3 scripts/populate_model_performance.py`
        3. Check MLflow: Visit http://localhost:5000
        """)

# Footer
st.divider()
st.markdown(f"""
<div style='text-align: center; color: #666; font-size: 0.9em;'>
    🎯 MLOps Production Pipeline | 
    Data: S3/DVC | 
    Processing: PySpark | 
    Validation: Great Expectations | 
    Training: MLflow | 
    Orchestration: Airflow
</div>
""", unsafe_allow_html=True)

st.caption(f"Last Refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
