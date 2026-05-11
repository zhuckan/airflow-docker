Airflow ML Pipeline: прогноз цен автомобилей

Автоматизация обучения и инференса модели машинного обучения с помощью Apache Airflow.

Что внутри

*   pipeline — загрузка данных, предобработка, обучение нескольких моделей (LogisticRegression, RandomForest, SVC), выбор лучшей по кросс-валидации и сохранение.
*   predict — загрузка сохранённой модели и предсказание для JSON-файлов из data/test.
*   DAG настроен на ежедневный запуск в 15:00.

Стек

Python · Airflow 3.2.1 · Docker Compose · Scikit-learn · Pandas · Dill · PostgreSQL · Redis

Быстрый старт

1. Клонируйте репозиторий:
bash
git clone <URL>
cd airflow-docker

2. Настройте переменные окружения:
bash
cp .env.example .env

3. Поднимите сервисы:
bash
docker compose up -d

4. Откройте веб-интерфейс Airflow:
Перейдите по адресу http://localhost:8080
Логин: airflow
Пароль: airflow

5.Активируйте DAG:
Найдите в списке car_price_prediction, включите тумблер и запустите вручную или дождитесь выполнения по расписанию.
