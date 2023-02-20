FROM ghcr.io/dbt-labs/dbt-snowflake:1.3.latest
WORKDIR /support
RUN mkdir /root/.dbt
COPY profiles.yml /root/.dbt
RUN mkdir /root/optimism
WORKDIR /optimism
COPY . .
EXPOSE 8080
ENTRYPOINT [ "bash"]