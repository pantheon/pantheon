#!/bin/ash
if [[ ! -z ${DEPLOYMENTURL} ]] && [[ ! -z ${TENANT} ]]; then
  echo "Found DEPLOYMENTURL and TENANT variables. Inserting them into index.html"

  if [[ -z ${TLS} ]]; then
  	echo "TLS variable not set. Assuming it's enabled."
  	PROTOCOL="https"
  elif [[ "${TLS}" == "ENABLED" ]]; then
  	echo "TLS enabled."
    PROTOCOL="https"
  else
  	echo "TLS disabled"
  	PROTOCOL="http"
  fi

  sed "s/base\ href=\"\/\"/base\ href=\"${PROTOCOL}:\/\/${DEPLOYMENTURL}\/${TENANT}\/\"/g" -i /usr/share/nginx/html/index.html
  ln -s /usr/share/nginx/html /usr/share/nginx/html/${TENANT}
else
  echo "DEPLOYMENTURL and TENANT variables are not set. Leaving index.html intact."
fi
echo "config.js:"
cat /usr/share/nginx/html/config.js
echo "Version:"
cat /version.txt
echo "Launching nginx"

nginx -g "daemon off;"