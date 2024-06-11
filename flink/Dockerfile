FROM flink:1.13.2-scala_2.12

COPY view_click_processor.py /opt/flink/jobs/

ENTRYPOINT ["flink", "run", "-d", "/opt/flink/jobs/view_click_processor.py"]
