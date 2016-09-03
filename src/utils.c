#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "utils.h"

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RESET   "\x1b[0m"

char* trim_newline(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '\r' || str[i] == '\n')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char* trim_whitespace(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!isspace(str[i])) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char* trim_parenthesis(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '(' || str[i] == ')')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char* trim_quotes(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (str[i] != '\"') {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void) out;
    (void) format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void) format;
#endif
}


