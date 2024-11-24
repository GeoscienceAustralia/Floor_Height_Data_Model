FROM python:3.11

WORKDIR /code

# copy only the requirements, this install step can take a while
# and requirements don't change as often as code, so doing it
# this way allows docker to cache the env
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# the backend/server application has some additional dependencies that
# build on that in the base requirements.txt (installed in above step)
COPY ./backend/requirements.txt /code/requirements-backend.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements-backend.txt

# copy the floor heights library
COPY ./src /code/floor-heights
ENV PYTHONPATH="/code/floor-heights:${PYTHONPATH}"

COPY ./backend/app /code/app
WORKDIR /code/

CMD ["uvicorn", "app.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8080"]
