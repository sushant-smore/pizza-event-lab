# 🍕 Pizza Event Lab

A simple event-driven pizza ordering and processing demo application.  
This project shows how orders can be created via a web interface and processed asynchronously — simulating a real-world event-driven architecture.

---

## 📂 Project Structure
├── Dockerfile # Builds the container for the app

├── entrypoint.sh # Startup script for container

├── processor.py # Processes pizza orders (event consumer)

├── requirements.txt # Python dependencies

└── webapp.py # Web application (order creation)


---

## 🚀 Features
- Create pizza orders from a web UI
- View orders
- Built with Python and Flask
- Easy Docker deployment

---

## 🛠️ Installation & Setup

1️⃣ Clone the repository
```bash
git clone https://github.com/sushant-smore/pizza-event-lab.git
cd pizza-event-lab

2️⃣ Install dependencies (if running locally)
pip install -r requirements.txt

3️⃣ Run the Web App
python webapp.py
App will be available at: http://localhost:5000

🐳 Running with Docker
docker build -t pizza-event-lab .
docker run -p 5000:5000 pizza-event-lab
