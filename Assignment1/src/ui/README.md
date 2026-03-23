# Streamlit UI - MLOps Movie Prediction Demo

This is the complete Streamlit-based user interface for the MLOps Movie Revenue Prediction system. It provides an interactive way to demonstrate all MLOps use cases.

## 🎯 Features

The UI includes 6 main sections:

### 1. **Dashboard** 🏠
- Real-time pipeline status
- Performance metrics and KPIs
- Recent predictions history
- Activity monitoring

### 2. **Use Case 1: New Movie Added to Catalog** ➕
- Interactive form to add new movies
- Real-time pipeline execution visualization
- Feature engineering display
- Prediction results with confidence scores
- Detailed model analysis

### 3. **Use Case 2: Batch Prediction** 📊
- CSV file upload for bulk predictions
- Sample template download
- Batch processing with progress tracking
- Results visualization (charts and tables)
- Export predictions to CSV

### 4. **Use Case 3: Model Retraining** 🔄
- Model performance comparison
- Training configuration
- Pipeline execution monitoring
- Performance metrics comparison
- Deployment options (staging/production)

### 5. **Use Case 4: A/B Testing** 🔀
- Compare multiple model versions
- Side-by-side predictions
- Performance vs latency trade-offs
- Traffic routing configuration
- Visual comparison charts

### 6. **Use Case 5: Drift Monitoring** 📈
- Feature drift detection
- Statistical test results
- Distribution comparison visualizations
- Drift timeline tracking
- Auto-retraining triggers

## 🚀 Quick Start

### Local Development (Recommended)

1. **Start all services:**
   ```powershell
   .\scripts\start.ps1
   ```

2. **Access the UI:**
   Open your browser to: http://localhost:8501

3. **Check status:**
   ```powershell
   .\scripts\status.ps1
   ```

4. **Stop services:**
   ```powershell
   .\scripts\stop.ps1
   ```

### First Time Setup

If this is your first time running the demo:

```powershell
# Run the complete setup script
.\scripts\setup_local.ps1
```

This will:
- Check prerequisites (Docker, Docker Compose)
- Install Python dependencies
- Create necessary directories
- Start all services
- Open the UI in your browser

## 📦 Architecture

```
┌─────────────────────────────────────────┐
│       Streamlit UI (Port 8501)          │
│  - Interactive Demo Interface           │
│  - Real-time Pipeline Visualization     │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│     Router Service (Port 80)            │
│  - A/B Testing                          │
│  - Traffic Splitting                    │
└─────────┬───────────────────────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌─────────┐ ┌─────────┐
│ API v1  │ │ API v2  │
│ :8000   │ │ :8001   │
└────┬────┘ └────┬────┘
     │           │
     └─────┬─────┘
           ▼
    ┌──────────────┐
    │   MLflow     │
    │   :5000      │
    └──────────────┘
```

## 🎬 Using the Demo

### Demo Use Case 1: Adding a New Movie

1. Navigate to the **"Use Case 1: New Movie"** tab
2. Fill in the movie details:
   - Title: "Inception 2"
   - Genre: "Sci-Fi"
   - Budget: $200,000,000
   - Runtime: 150 minutes
   - Release Year: 2024
3. Click **"Run MLOps Pipeline"**
4. Watch the pipeline execution in real-time
5. View prediction results and model confidence

### Demo Use Case 2: Batch Predictions

1. Navigate to the **"Use Case 2: Batch Prediction"** tab
2. Download the sample CSV template
3. Or upload the pre-made sample: `data/raw/sample_batch_movies.csv`
4. Click **"Predict All"**
5. View results table and visualizations
6. Download results as CSV

### Demo Use Case 3: Model Retraining

1. Navigate to the **"Use Case 3: Model Retraining"** tab
2. Review current model vs candidate model comparison
3. Adjust training parameters (optional)
4. Click **"Trigger Retraining Pipeline"**
5. Watch training logs in real-time
6. Review performance improvements
7. Deploy to staging or production

### Demo Use Case 4: A/B Testing

1. Navigate to the **"Use Case 4: A/B Testing"** tab
2. Enter a test movie
3. Click **"Compare All Models"**
4. View predictions from 3 different models
5. Analyze trade-offs (accuracy vs latency)
6. Adjust traffic routing percentages

### Demo Use Case 5: Drift Monitoring

1. Navigate to the **"Use Case 5: Drift Monitoring"** tab
2. View drift alerts for all features
3. Select a feature to analyze in detail
4. View distribution comparison charts
5. Check statistical test results
6. Trigger retraining if drift detected

## 🎨 Customization

### Theme Configuration

Edit `.streamlit/config.toml` to customize colors and appearance:

```toml
[theme]
primaryColor = "#FF4B4B"  # Red for movie theme
backgroundColor = "#0E1117"  # Dark background
```

### Environment Variables

Edit `.env` file to configure service URLs:

```env
API_URL=http://localhost:80
MLFLOW_TRACKING_URI=http://localhost:5000
AIRFLOW_URL=http://localhost:8080
```

## 🔧 Troubleshooting

### UI Not Loading

```powershell
# Check if service is running
docker ps | findstr streamlit

# View logs
docker-compose logs -f streamlit-ui

# Restart service
docker-compose restart streamlit-ui
```

### Cannot Connect to API

```powershell
# Check API service status
docker-compose ps api-service

# Test API endpoint
curl http://localhost:8000/health

# Check router service
docker-compose logs router-service
```

### Port Already in Use

If port 8501 is already in use, you can change it in `docker-compose.yml`:

```yaml
streamlit-ui:
  ports:
    - "8502:8501"  # Change to different port
```

## 📊 Monitoring

### View Service Logs

```powershell
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f streamlit-ui

# Last 100 lines
docker-compose logs --tail=100 streamlit-ui
```

### Resource Usage

```powershell
# Check resource usage
docker stats

# Or use the status script
.\scripts\status.ps1
```

## 🚢 Deployment

### Local (Current)
- URL: http://localhost:8501
- Access: Your machine only
- Cost: $0

### Streamlit Community Cloud (Free)
1. Push code to GitHub
2. Connect repository at streamlit.io/cloud
3. Set environment variables
4. Deploy!
- URL: https://your-app.streamlit.app
- Access: Public
- Cost: $0

### EKS (Production)
See `k8s-minimal/streamlit-deployment.yaml` for Kubernetes deployment
- URL: Your Load Balancer URL or custom domain
- Access: Configurable (public/private)
- Cost: ~$20-30/month

## 📝 Notes

- The UI uses demo/mock data for visualization when services aren't fully connected
- All predictions are simulated based on simple formulas in demo mode
- For production use, connect to real MLflow tracking server and model registry
- Authentication is disabled by default for demo purposes

## 🆘 Support

For issues or questions:
1. Check the logs: `docker-compose logs -f streamlit-ui`
2. Verify all services are running: `.\scripts\status.ps1`
3. Review the README.md in the root directory

## 📚 Additional Resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [MLflow Documentation](https://mlflow.org/docs/latest/index.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
