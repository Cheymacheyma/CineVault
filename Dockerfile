FROM python:3.10-slim

# تثبيت أداة التحميل aria2 كما كنت تفعل في القديم
RUN apt-get update && apt-get install -y aria2 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# المنفذ الافتراضي (يمكنك جعله 8000 أو 7860 حسب ما تطعمه المنصة)
EXPOSE 8000

# استخدام gunicorn للإنتاج لضمان عدم توقف السيرفر
CMD ["gunicorn", "app:app", "--bind", "0.0.0.0:8000"]
