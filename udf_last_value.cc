/*
  returns the last value in a group
  input parameters:
  data (real or int -- must be numeric, doesn't work for strings)
  output:
  last value of the group (real)
  compiling:
  gcc -fPIC -Wall -I /usr/include/mysql/ -shared -o udf_last_value.so  udf_last_value.cc
  cp udf_last_value.so /usr/lib64/mysql/plugin/
  registering the function:
  CREATE AGGREGATE FUNCTION last_value RETURNS REAL SONAME 'udf_last_value.so';
  getting rid of the function:
  DROP FUNCTION last_value;
*/


#ifdef STANDARD
#include <stdio.h>
#include <string.h>
#ifdef __WIN__
typedef unsigned __int64 ulonglong;
typedef __int64 longlong;
#else
typedef unsigned long long ulonglong;
typedef long long longlong;
#endif /*__WIN__*/
#else
#include <my_global.h>
#include <my_sys.h>
#endif
#include <mysql.h>
#include <m_ctype.h>
#include <m_string.h>

#ifdef HAVE_DLOPEN


#define BUFFERSIZE 1024



extern "C" {
my_bool last_value_init( UDF_INIT* initid, UDF_ARGS* args, char* message );
void last_value_deinit( UDF_INIT* initid );
void last_value_reset( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error );
void last_value_clear(UDF_INIT* initid, char *is_null, char *error);
void last_value_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error );
double last_value( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char *error );
}


struct last_value_data
{
  unsigned long count;
  unsigned long abscount;
  unsigned long pages;
  double *values;
};


my_bool last_value_init( UDF_INIT* initid, UDF_ARGS* args, char* message )
{
  if ( args->arg_count < 1 || args->arg_count>1)
  {
    strcpy(message,"wrong number of arguments: last_value() requires one argument");
    return 1;
  }

  last_value_data *buffer = new last_value_data;
  buffer->count = 0;
  buffer->abscount=0;
  buffer->pages = 1;
  buffer->values = NULL;

  initid->maybe_null    = 1;
  initid->max_length    = 32;
  initid->ptr = (char*)buffer;

  return 0;
}


void last_value_deinit( UDF_INIT* initid )
{
  last_value_data *buffer = (last_value_data*)initid->ptr;

  if (buffer->values != NULL)
  {
    free(buffer->values);
    buffer->values=NULL;
  }
  delete initid->ptr;
}

void last_value_clear(UDF_INIT* initid, char *is_null, char *error){
  last_value_data *buffer = (last_value_data*)initid->ptr;
  buffer->count = 0;
  buffer->abscount=0;
  buffer->pages = 1;
  *is_null = 0;
  *error = 0;

  if (buffer->values != NULL)
  {
    free(buffer->values);
    buffer->values=NULL;
  }

  buffer->values=(double *) malloc(BUFFERSIZE*sizeof(double));
}

void last_value_reset( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error )
{
  last_value_clear(initid, is_null, is_error);
  last_value_add( initid, args, is_null, is_error );
}


void last_value_add( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error )
{
  if (args->args[0]!=NULL)
  {
    last_value_data *buffer = (last_value_data*)initid->ptr;
    if (buffer->count>=BUFFERSIZE)
    {
      buffer->pages++;
      buffer->count=0;
      buffer->values=(double *) realloc(buffer->values,BUFFERSIZE*buffer->pages*sizeof(double));
    }
    if(args->arg_type[0]==INT_RESULT){
      buffer->values[buffer->abscount++] = (double) *((longlong*)args->args[0]);
    }
    else{
      buffer->values[buffer->abscount++] = *((double*)args->args[0]);
    }
    buffer->count++;
  }
}


double last_value( UDF_INIT* initid, UDF_ARGS* args, char* is_null, char* is_error )
{
  last_value_data* buffer = (last_value_data*)initid->ptr;

  if (buffer->abscount==0 || *is_error!=0)
  {
    *is_null = 1;
    return 0.0;
  }

  *is_null=0;

  int n = buffer->abscount - 1;

  return buffer->values[n];

}

#endif
