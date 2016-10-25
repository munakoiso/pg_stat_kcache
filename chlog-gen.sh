cat > debian/changelog<<EOH
postgresql-9.5-kcache (2.0.3-$(git rev-parse --short HEAD)) trusty; urgency=low

  * Yandex qutobuild

 -- ${USER} <${USER}@$(hostname)>  $(date +%a\,\ %d\ %b\ %Y\ %H:%M:%S\ %z)
EOH
