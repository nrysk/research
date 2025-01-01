FROM python:3.12

WORKDIR /workspace

COPY . .

RUN python -m pip install -r requirements.txt

RUN python -m pip install jupyter

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser", "--NotebookApp.token=''"]