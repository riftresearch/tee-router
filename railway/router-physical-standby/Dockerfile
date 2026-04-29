FROM postgres:18

COPY entrypoint.sh /usr/local/bin/router-physical-standby-entrypoint.sh
RUN chmod +x /usr/local/bin/router-physical-standby-entrypoint.sh

ENTRYPOINT ["router-physical-standby-entrypoint.sh"]
CMD ["postgres"]
