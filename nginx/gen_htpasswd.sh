#!/bin/sh
HASH=$(openssl passwd -apr1 admin123)
echo "admin:${HASH}" > /etc/nginx/.htpasswd
cat /etc/nginx/.htpasswd
echo "Done!"
