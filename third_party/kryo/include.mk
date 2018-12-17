# Copyright (C) 2017  The OpenTSDB Authors.
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

KRYO_VERSION := 2.21.1
KRYO := third_party/kryo/kryo-$(KRYO_VERSION).jar
KRYO_BASE_URL := http://central.maven.org/maven2/com/esotericsoftware/kryo/kryo/$(KRYO_VERSION)

$(KRYO): $(KRYO).md5
	set dummy "$(KRYO_BASE_URL)" "$(KRYO)"; shift; $(FETCH_DEPENDENCY)

REFLECTASM_VERSION := 1.07
REFLECTASM := third_party/kryo/reflectasm-$(REFLECTASM_VERSION)-shaded.jar
REFLECTASM_BASE_URL := http://central.maven.org/maven2/com/esotericsoftware/reflectasm/reflectasm/$(REFLECTASM_VERSION)

$(REFLECTASM): $(REFLECTASM).md5
	set dummy "$(REFLECTASM_BASE_URL)" "$(REFLECTASM)"; shift; $(FETCH_DEPENDENCY)

ASM_VERSION := 4.0
ASM := third_party/kryo/asm-$(ASM_VERSION).jar
ASM_BASE_URL := http://central.maven.org/maven2/org/ow2/asm/asm/$(ASM_VERSION)

$(ASM): $(ASM).md5
	set dummy "$(ASM_BASE_URL)" "$(ASM)"; shift; $(FETCH_DEPENDENCY)

MINLOG_VERSION := 1.2
MINLOG := third_party/kryo/minlog-$(MINLOG_VERSION).jar
MINLOG_BASE_URL := http://central.maven.org/maven2/com/esotericsoftware/minlog/minlog/$(MINLOG_VERSION)

$(MINLOG): $(MINLOG).md5
	set dummy "$(MINLOG_BASE_URL)" "$(MINLOG)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(KRYO) $(REFLECTASM) $(ASM) $(MINLOG)