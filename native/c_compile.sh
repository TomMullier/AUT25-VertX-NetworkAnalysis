#!/bin/bash
clear
echo "=== Compiling JNI library ==="

JAVA_INCLUDE_PATH=$(dirname $(find /usr/lib/jvm -type f -name jni.h | head -n 1))
JAVA_BASE_PATH=$(dirname "$JAVA_INCLUDE_PATH")

gcc -fPIC \
  -I"$JAVA_BASE_PATH/include" \
  -I"$JAVA_BASE_PATH/include/linux" \
  -I/usr/local/include/ndpi \
  -I/usr/local/include/ndpi/src/include \
  -I/usr/local/include \
  -I/usr/include \
  -I/usr/lib/gcc/x86_64-redhat-linux/14/include \
  -shared -o libndpi_jni.so ndpi_jni.c -lndpi \
  && echo "✅ Compilation successful!" \
  || echo "❌ Compilation failed."
