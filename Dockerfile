FROM python:3.12

ENV TZ=Asia/Tokyo

WORKDIR /workspace

COPY . .

RUN apt-get update && apt-get install fonts-noto-cjk -y && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN python -m pip install -r requirements.txt

RUN python -m pip install jupyter

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--allow-root", "--no-browser", "--NotebookApp.token=''"]