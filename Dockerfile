FROM prefecthq/prefect:0.15.7

ARG DISCORD_HOOK_URL
ENV DISCORD_HOOK $DISCORD_HOOK_URL

RUN pip install --no-cache-dir -U "poetry==1.1.11" "pip>=21.2.4"
WORKDIR /app
COPY . .
RUN pip3 install --no-cache-dir -U .
