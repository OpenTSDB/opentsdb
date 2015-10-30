# Copyright (C) 2015  The OpenTSDB Authors.
#
# This library is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation, either version 2.1 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this library.  If not, see <http://www.gnu.org/licenses/>.

# ALPN_BOOT_VERSION := 7.1.3.v20150130
ALPN_BOOT_VERSION = $(shell version= ;\
  if [[ "@JAVA@" ]]; then \
    version=$$("@JAVA@" -version 2>&1 | awk -F '"' '/version/ {print $$2}'); \
  else\
    echo "Failed to parse Java version";\
    exit 1;\
  fi; \
  if [[ $$version =~ ^([0-9]+\.[0-9]+)\.([0-9])[_Uu]([0-9]+)$$ ]]; then \
    major=$${BASH_REMATCH[1]};\
    minor=$${BASH_REMATCH[2]}; \
    sub=$${BASH_REMATCH[3]}; \
  if [[ $$major = "1.7" ]]; then \
    if [[ $$sub < 71 ]]; then \
      echo "7.1.0.v20141016"; \
    elif [[ $$sub < 75 ]]; then \
      echo "7.1.2.v20141202"; \
    else \
       echo "7.1.3.v20150130"; \
    fi \
  elif [[ $$major = "1.8" ]]; then \
    if [[ $$sub < 25 ]]; then \
      echo "8.1.0.v20141016"; \
    elif [[ $$sub < 31 ]]; then \
      echo "8.1.2.v20141202"; \
    elif [[ $$sub < 51 ]]; then \
       echo "8.1.3.v20150130"; \
    elif [[ $$sub < 60 ]]; then \
      echo "8.1.4.v20150727"; \
    else \
      echo "8.1.5.v20150921"; \
    fi \
  else \
    echo "Unsupported major Java version: $$major"; \
    exit 1; \
  fi \
  else \
  echo "Possibly invalid Java version (couldn't parse): $$version"; \
  exit 1; \
  fi)


ALPN_BOOT := third_party/alpn-boot/alpn-boot-$(ALPN_BOOT_VERSION).jar
ALBPN_BOOT_BASE_URL := http://central.maven.org/maven2/org/mortbay/jetty/alpn/alpn-boot/$(ALPN_BOOT_VERSION)

$(ALPN_BOOT): $(ALPN_BOOT).md5
	set dummy "$(ALBPN_BOOT_BASE_URL)" "$(ALPN_BOOT)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(ALPN_BOOT)
