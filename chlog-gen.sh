cat > debian/changelog<<EOH
postgresql-${MAJOR}-kcache (2.1.1-$(git rev-parse --short HEAD)) trusty; urgency=low

  * Yandex autobuild

 -- ${USER} <${USER}@$(hostname)>  $(date +%a\,\ %d\ %b\ %Y\ %H:%M:%S\ %z)
EOH

sed -i s/REPLACE_ME/$MAJOR/g debian/control
