#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "jp_ac_titech_cs_de_ykstorage_storage_datadisk_RAPoSDADataDiskManager.h"

#define BLOCK_SIZE 512

long get_buffer_size(long data_size)
{
  return (long)(ceil((double)data_size / BLOCK_SIZE) * BLOCK_SIZE);
}

void debug_buffer_contents(jbyte* buffer, long size)
{
  printf("buf contents: \n");
  for (int i=0; i < size; i++) {
    printf("%02X ", (char)buffer[i]);
  }
  printf("\n");
}

/*
 * Class:     jp_ac_titech_cs_de_ykstorage_storage_datadisk_RAPoSDADataDiskManager
 * Method:    write
 * Signature: (Ljava/lang/String;[B)Z
 */
JNIEXPORT jboolean JNICALL
Java_jp_ac_titech_cs_de_ykstorage_storage_datadisk_RAPoSDADataDiskManager_write
(JNIEnv *env, jobject thisObj, jstring filePath, jbyteArray byteArray)
{
  int fd;
  jbyte *buf;
  const char *utf_file_path;
  jboolean isCopy, retVal;
  long ret, buf_size;

  buf_size = get_buffer_size(env->GetArrayLength(byteArray) * sizeof(jbyte));

  posix_memalign((void**)&buf, BLOCK_SIZE, buf_size);
  // printf("aligned buffer address: %p, ", buf);
  // printf("buffer size: %ld\n", buf_size);

  utf_file_path = env->GetStringUTFChars(filePath, &isCopy);
  // printf("write to: %s\n", utf_file_path);

  fd = open(utf_file_path, O_WRONLY|O_DIRECT|O_SYNC|O_CREAT, S_IRWXU);
  if (isCopy == JNI_TRUE)
    env->ReleaseStringUTFChars(filePath, utf_file_path);

  memset(buf, 0, buf_size);
  // debug_buffer_contents(buf, buf_size);
  env->GetByteArrayRegion(
			  byteArray,
			  0,
			  env->GetArrayLength(byteArray),
			  buf);

  ret = write(fd, buf, buf_size);
  if (ret >= 0) {
    // printf("write successful. written bytes: %ld\n", ret);
    retVal = JNI_TRUE;
  } else {
    printf("[JNI] write failed: %s\n", strerror(errno));
    retVal = JNI_FALSE;
  }

  ret = close(fd);
  if (ret < 0) {
    printf("[JNI] couldn't close the file: %s\n", strerror(errno));
    retVal = JNI_FALSE;
  }

  free(buf);
  return retVal;
}

/*
 * Class:     jp_ac_titech_cs_de_ykstorage_storage_datadisk_RAPoSDADataDiskManager
 * Method:    read
 * Signature: (Ljava/lang/String;)[B
 */
JNIEXPORT jbyteArray JNICALL
Java_jp_ac_titech_cs_de_ykstorage_storage_datadisk_RAPoSDADataDiskManager_read
(JNIEnv *env, jobject thisObj, jstring filePath)
{
  long file_size, buf_size, ret;
  struct stat statbuf;
  int fd;
  // char *buf;
  jbyte *buf;
  const char *utf_file_path;
  jboolean isCopy;
  jbyteArray retVal;

  // get the file size
  utf_file_path = env->GetStringUTFChars(filePath, &isCopy);
  // printf("read from: %s\n", utf_file_path);

  fd = open(utf_file_path, O_RDONLY|O_DIRECT|O_SYNC);
  if (isCopy == JNI_TRUE) {
    env->ReleaseStringUTFChars(filePath, utf_file_path);
  }
  fstat(fd, &statbuf);

  file_size = statbuf.st_size;
  // printf("read file size: %ld\n", file_size);

  buf_size = get_buffer_size(file_size);
  // printf("buffer size: %ld\n", buf_size);

  // align buffer
  posix_memalign((void**)&buf, BLOCK_SIZE, buf_size);
  
  ret = read(fd, buf, buf_size);
  if (ret >= 0) {
    // printf("read successful. read bytes: %ld\n", ret);

    // convert native byte array to java byte array
    retVal = env->NewByteArray(buf_size / sizeof(jbyte));
    env->SetByteArrayRegion(
			    (jbyteArray)retVal,
			    0,
			    buf_size / sizeof(jbyte),
			    buf);
  } else {
    printf("[JNI] read failed: %s\n", strerror(errno));
    retVal = env->NewByteArray(0);
  }

  // printf("this print is normal?\n");

  ret = close(fd);
  if (ret < 0) {
    printf("[JNI] couldn't close the file: %s\n", strerror(errno));
  }

  free(buf);
  return retVal;
}
