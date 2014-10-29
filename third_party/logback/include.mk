# Copyright (C) 2011-2012  The Async HBase Authors.  All rights reserved.
# This file is part of Async HBase.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#   - Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   - Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#   - Neither the name of the StumbleUpon nor the names of its contributors
#     may be used to endorse or promote products derived from this software
#     without specific prior written permission.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

LOGBACK_VERSION := 1.0.13

LOGBACK_CLASSIC_VERSION := $(LOGBACK_VERSION)
LOGBACK_CLASSIC := third_party/logback/logback-classic-$(LOGBACK_CLASSIC_VERSION).jar
LOGBACK_CLASSIC_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(LOGBACK_CLASSIC): $(LOGBACK_CLASSIC).md5
	set dummy "$(LOGBACK_CLASSIC_BASE_URL)" "$(LOGBACK_CLASSIC)"; shift; $(FETCH_DEPENDENCY)


LOGBACK_CORE_VERSION := $(LOGBACK_VERSION)
LOGBACK_CORE := third_party/logback/logback-core-$(LOGBACK_CORE_VERSION).jar
LOGBACK_CORE_BASE_URL := $(OPENTSDB_THIRD_PARTY_BASE_URL)

$(LOGBACK_CORE): $(LOGBACK_CORE).md5
	set dummy "$(LOGBACK_CORE_BASE_URL)" "$(LOGBACK_CORE)"; shift; $(FETCH_DEPENDENCY)

THIRD_PARTY += $(LOGBACK_CLASSIC) $(LOGBACK_CORE)
