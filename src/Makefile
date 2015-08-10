#-------------------------------------------------------------------------------
# Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
# All rights reserved.
#  
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#  
#   * Redistributions of source code must retain the above copyright notice,
#     this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of Redis nor the names of its contributors may be used
#     to endorse or promote products derived from this software without
#     specific prior written permission.
#  
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
# BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
# THE POSSIBILITY OF SUCH DAMAGE.
#-------------------------------------------------------------------------------
#Makefile
#
# Created on: 2013-3-28
#     Author: yinqiwen

CXX=g++
CC=gcc

COMMS_VERSION=0.1.0

uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

LIB_PATH=$(PWD)/../deps

JEMALLOC_VER=jemalloc-3.6.0
JEMALLOC_PATH=${LIB_PATH}/${JEMALLOC_VER}
JEMALLOC_FILE=${JEMALLOC_PATH}.tar.bz2
JEMALLOC_LIBA=${JEMALLOC_PATH}/lib/libjemalloc.a

LUA_VER=lua
LUA_PATH=${LIB_PATH}/${LUA_VER}
#LUA_FILE=${LUA_PATH}.tar.gz
LUA_LIBA=${LUA_PATH}/src/liblua.a

MMKV_VER=mmkv
MMKV_PATH=${LIB_PATH}/${MMKV_VER}
MMKV_LIBA=${MMKV_PATH}/src/libmmkv.a

OPTIMIZATION?=-O2
OPT=$(OPTIMIZATION)

CXXFLAGS=-Wall -g ${OPT} -fPIC -D__STDC_FORMAT_MACROS -DCOMMS_VERSION='"${COMMS_VERSION}"' 
CCFLAGS=-Wall -std=gnu99 ${OPT} -fPIC -pedantic -g -D__STDC_FORMAT_MACROS -DCOMMS_VERSION='"${COMMS_VERSION}"'
LDFLAGS=-g 


LUA_CFLAGS+= $(OPT) -Wall -DLUA_ANSI $(CCFLAGS)
LUA_LDFLAGS+= $(LDFLAGS)

SPARSEHASH_VER=sparsehash-2.0.2
SPARSEHASH_PATH=${LIB_PATH}/${SPARSEHASH_VER}
SPARSEHASH_FILE=${SPARSEHASH_PATH}.tar.gz
SPARSEHASH_CONFIG=${SPARSEHASH_PATH}/src/config.h

BOOST_INC?=/usr/include
INCS=-I./ -I./common -I${LIB_PATH}/cpp-btree -I${SPARSEHASH_PATH}/src/ -I${MMKV_PATH}/src -I${LUA_PATH}/src  -I${BOOST_INC} 

# Default allocator
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
else
	MALLOC=libc
endif


ifeq ($(MALLOC),libc)
#do nothing
else
MALLOC_LIBA=${JEMALLOC_LIBA}
DEP_LIBS+=jemalloc
endif

LIBS= ${LUA_LIBA} ${MMKV_LIBA}  ${MALLOC_LIBA} ${SNAPPY_LIBA} -lpthread

%.o : %.cpp
	${CXX} -c ${CXXFLAGS} ${INCS} $< -o $@

%.o : %.c
	${CC} -c ${CCFLAGS} ${INCS} $< -o $@


CHANNEL_CPP_VPATH=common/channel common/channel/socket common/channel/fifo common/channel/codec common/channel/timer common/channel/signal
CHANNEL_C_VPATH=common/channel/redis 
CHANNEL_CPPFILES := $(foreach dir, $(CHANNEL_CPP_VPATH), $(wildcard $(dir)/*.cpp))
CHANNEL_CFILES := $(foreach dir, $(CHANNEL_C_VPATH), $(wildcard $(dir)/*.c))
CHANNEL_OBJECTS := $(patsubst %.cpp, %.o, $(CHANNEL_CPPFILES)) $(patsubst %.c, %.o, $(CHANNEL_CFILES))

COMMON_VPATH=common/util common/util/exception  common/thread  common/redis common/buffer
COMMON_CPPFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.cpp))
COMMON_CFILES := $(foreach dir, $(COMMON_VPATH), $(wildcard $(dir)/*.c))
COMMON_OBJECTS := $(patsubst %.cpp, %.o, $(COMMON_CPPFILES)) $(patsubst %.c, %.o, $(COMMON_CFILES))

COMMAND_VPATH=command
COMMAND_CPPFILES := $(foreach dir, $(COMMAND_VPATH), $(wildcard $(dir)/*.cpp))
COMMAND_OBJECTS := $(patsubst %.cpp, %.o, $(COMMAND_CPPFILES))

REPL_VPATH=replication
REPL_CPPFILES := $(foreach dir, $(REPL_VPATH), $(wildcard $(dir)/*.cpp))
REPL_CFILES := $(foreach dir, $(REPL_VPATH), $(wildcard $(dir)/*.c))
REPL_OBJECTS := $(patsubst %.cpp, %.o, $(REPL_CPPFILES)) $(patsubst %.c, %.o, $(REPL_CFILES))

CORE_OBJECTS := main.o comms.o config.o cron.o logger.o network.o  statistics.o  \
                $(COMMON_OBJECTS) $(CHANNEL_OBJECTS) $(COMMAND_OBJECTS) $(REPL_OBJECTS) 


all: ${PREBUILD}  server 

lib: $(DEP_LIBS) lua sparsehash mmkv

server: ${STORAGE_ENGINE_OBJ} lib $(CORE_OBJECTS) ${SERVEROBJ}
	${CXX} -o comms-server $(SERVEROBJ)  $(CORE_OBJECTS) ${STORAGE_ENGINE_OBJ} $(LIBS)


.PHONY: jemalloc
jemalloc: $(JEMALLOC_LIBA)
$(JEMALLOC_LIBA):
	echo ">>>>> Building JEMALLOC" && \
	cd ${LIB_PATH} && \
	tar jxf ${JEMALLOC_FILE} && \
	cd ${JEMALLOC_PATH} && \
	./configure  && \
	$(MAKE) build_lib_static && \
	echo "<<<<< Done building JEMALLOC"

.PHONY: lua
lua: $(LUA_LIBA)
$(LUA_LIBA):
	echo ">>>>> Building LUA" && \
	cd ${LIB_PATH} && \
	cd ${LUA_PATH}/src && \
	$(MAKE) all CFLAGS="$(LUA_CFLAGS)" MYLDFLAGS="$(LUA_LDFLAGS)" && \
	echo ">>>>> Done building LUA"

.PHONY: sparsehash
sparsehash: $(SPARSEHASH_CONFIG)
$(SPARSEHASH_CONFIG):
	echo ">>>>> Building sparsehash" && \
	cd ${LIB_PATH}; tar zxf ${SPARSEHASH_FILE};\
	cd ${SPARSEHASH_PATH}; ./configure;  \
	${MAKE} src/sparsehash/internal/sparseconfig.h && \
	echo ">>>>> Done building sparsehash"

.PHONY: mmkv
mmkv: $(MMKV_LIBA)
$(MMKV_LIBA):
	echo ">>>>> Building MMKV" && \
	cd ${MMKV_PATH} && \
	cd ${MMKV_PATH}/src && \
	$(MAKE) && \
	echo ">>>>> Done building MMKV"

clean_deps:
	rm -rf $(JEMALLOC_PATH); \
	$(MAKE) -C $(LUA_PATH) clean

noopt:
	$(MAKE) OPT="-O0"

valgrind:
	$(MAKE) OPT="-O0" MALLOC="libc"

dist:clean all
	rm -rf comms-${COMMS_VERSION};mkdir -p comms-${COMMS_VERSION}/bin comms-${COMMS_VERSION}/conf comms-${COMMS_VERSION}/logs comms-${COMMS_VERSION}/data comms-${COMMS_VERSION}/repl comms-${COMMS_VERSION}/backup; \
	cp comms-server comms-${COMMS_VERSION}/bin; cp comms-test comms-${COMMS_VERSION}/bin; cp ../comms.conf comms-${COMMS_VERSION}/conf; \
	tar czvf comms-bin-${COMMS_VERSION}.tar.gz comms-${COMMS_VERSION}; rm -rf comms-${COMMS_VERSION};

clean:
	rm -f  ${CORE_OBJECTS} $(STORAGE_ENGINE_OBJ) $(SERVEROBJ) comms-server

clobber: clean_deps clean
