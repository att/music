FROM busybox:latest
RUN mkdir -p app 
COPY maven/ app/
CMD ["tail", "-f", "/dev/null"]
