#define _XOPEN_SOURCE
#define _BSD_SOURCE
/**
 * client.c
 *  CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/   
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        perror("client connect failed: ");
        return -1;
    }

    log_info("Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

void parse_load_query(char* loadQuery, int client_socket){
    char *temp, *tempToFree;
    tempToFree = temp = (char*)strdup(loadQuery);
    //cs165_log(stdout, loadQuery);
    temp += 4;
    if (strncmp(temp, "(", 1) == 0) {
        char* filePathWithQuotes = trim_parenthesis(temp);
        char* filePath = trim_quotes(filePathWithQuotes);
        int last_char = strlen(filePath) - 1;
        filePath[last_char] = '\0';
        int len = 0;

        message load_send_message;
        message load_recv_message;
        FILE* fd = fopen(filePath, "r");
        fseek(fd, 0, SEEK_END);
        unsigned long actual_size = ftell(fd);
        rewind(fd);

        //printf("In file size = %ld bytes.\n", actual_size);
        size_t unit_size = 1<<12;
        size_t num_chunks = 0;
        num_chunks = actual_size / unit_size;
        //size_t last_chunk_size = actual_size % unit_size;   

        if(fd != NULL){
            char buf[1024]; //TO CHECK: If buffer size if enough?
            char table_Name[256];
            char query_insert[1024];
            memset(buf, '\0', 1024);
            memset(table_Name, '\0', 256);
            memset(query_insert, '\0', 1024);

            //Read all the column names from the first line
            char** col_names = (char**)malloc(sizeof(char*) * 10);
            int nCols=0, i=0; //Number of Columns
            if(fgets(buf, 1024, fd))
            {
                char* token = NULL;
                char* tmp, *tmpToFree;
                tmp = tmpToFree = strdup(buf);
                while ((token = strsep(&tmp, ",")) != NULL) {
                    col_names[nCols] = (char*)malloc(sizeof(char) * (strlen(token) + 1));
                    strcpy(col_names[nCols], token);
                    nCols++;
                }
                int last_char = strlen(col_names[nCols-1]) - 1;
                col_names[nCols-1][last_char] = '\0';
                free(tmpToFree);
            }

            size_t col_val_capacity = unit_size;
            char** col_values = (char**)malloc(sizeof(char*) * nCols);
            for(i=0; i<nCols; i++){
                col_values[i] = (char*)malloc(sizeof(char) * col_val_capacity);
                memset(col_values[i], '\0', col_val_capacity);
                strcpy(col_values[i], "load(");
                strcat(col_values[i], col_names[i]);
                strcat(col_values[i], ",");
            }
            
            size_t string_curr_length = 0;

            if(num_chunks == 0){
                while (fgets(buf, 1024, fd)){
                    int j=0;
                    char* token = NULL;
                    char* tmp, *tmpToFree;
                    tmp = tmpToFree = strdup(buf);
                    trim_newline(tmp);
                    if(string_curr_length > col_val_capacity * 0.8 ){
                        col_val_capacity *= 20; 
                        for(i=0; i<nCols; i++){
                             char* temp_col_values = (char*)realloc(col_values[i], col_val_capacity * sizeof(char));
                             col_values[i] = temp_col_values;
                        }
                    }
                    size_t token_len = 0;
                    while ((token = strsep(&tmp, ",")) != NULL && (token_len = strlen(token)) > 0) {
                        string_curr_length += token_len;
                        strcat(col_values[j], token);
                        strcat(col_values[j], ",");
                        
                        j++;
                    }
                }
                 size_t payload_length = 0;
                for(i=0; i<nCols; i++){
                    size_t len = strlen(col_values[i]);
                    int last_char = len - 1;
                    col_values[i][last_char] = '\0';
                    payload_length += len;
                }
                for(i=0; i<nCols; i++){
                    if(i == nCols-1)
                        strcat(col_values[i], ")\n");
                    else
                        strcat(col_values[i], "),");
                }
                
                char *payload = NULL;
                payload_length += (nCols*5);
                payload = (char*) malloc(sizeof(char)* (payload_length));
                memset(payload, '\0', (payload_length));

                for(i=0; i<nCols; i++){
                    strcat(payload, col_values[i]);
                }
                load_send_message.length = payload_length;

                //cs165_log(stdout, payload);

                if (send(client_socket, &(load_send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message header.");
                    exit(1);
                }

                // Send the payload (query) to server
                if (send(client_socket, payload, load_send_message.length, 0) == -1) {
                    log_err("Failed to send query payload.");
                    exit(1);
                }
                
                // Always wait for server response (even if it is just an OK message)
                if ((len = recv(client_socket, &(load_recv_message), sizeof(message), 0)) > 0) {
                    if (load_recv_message.status == OK_WAIT_FOR_RESPONSE &&
                        (int) load_recv_message.length > 0) {
                        // Calculate number of bytes in response package
                        int num_bytes = (int) load_recv_message.length;
                        char payloadRecv[num_bytes + 1];

                        // Receive the payload and print it out
                        if ((len = recv(client_socket, payloadRecv, num_bytes, 0)) > 0) {
                            payloadRecv[num_bytes] = '\0';
                            printf("%s\n", payloadRecv);
                        }
                    }
                }
                else {
                    if(payload != NULL)
                        free(payload);
                    if (len < 0) {
                        log_err("Failed to receive message.");
                    }
                    else {
                        log_info("Server closed connection\n");
                    }
                    exit(1);
                }

                if(payload != NULL)
                    free(payload);

            }
            else{
                while (fgets(buf, 1024, fd)){
                    int j=0;
                    char* token = NULL;
                    char* tmp, *tmpToFree;
                    tmp = tmpToFree = strdup(buf);
                    trim_newline(tmp);
                
                    size_t token_len = 0;
                    while ((token = strsep(&tmp, ",")) != NULL && (token_len = strlen(token)) > 0) {
                        string_curr_length += token_len;
                        strcat(col_values[j], token);
                        strcat(col_values[j], ",");
                        j++;
                    }
                    memset(buf, '\0', 1024);
                    if(string_curr_length >= (0.75 * unit_size)){
                        size_t payload_length = 0;
                        for(i=0; i<nCols; i++){
                            size_t len = strlen(col_values[i]);
                            int last_char = len - 1;
                            //col_values[i][last_char] = '\0';
                            if(i == nCols-1)
                            {
                                col_values[i][last_char] = ')';
                                col_values[i][last_char+1] = '\n';
                                col_values[i][last_char+2] = '\0';
                            }
                            else
                            {
                                col_values[i][last_char] = ')';
                                col_values[i][last_char+1] = ',';
                                col_values[i][last_char+2] = '\0';
                            }
                            payload_length += (len+2);
                        }
        
                        char *payload = NULL;
                        payload_length += (nCols*5);
                        payload = (char*) malloc(sizeof(char)* (payload_length));
                        memset(payload, '\0', (payload_length));

                        for(i=0; i<nCols; i++){
                            strcat(payload, col_values[i]);
                        }
                        load_send_message.length = payload_length;

                        //cs165_log(stdout, payload);
                       // printf("payload_length:%zu\n", payload_length);
                        if (send(client_socket, &(load_send_message), sizeof(message), 0) == -1) {
                            log_err("Failed to send message header.");
                            exit(1);
                        }

                        // Send the payload (query) to server
                        if (send(client_socket, payload, load_send_message.length, 0) == -1) {
                            log_err("Failed to send query payload.");
                            exit(1);
                        }
                        
                        // Always wait for server response (even if it is just an OK message)
                        if ((len = recv(client_socket, &(load_recv_message), sizeof(message), 0)) > 0) {
                            //printf("Length Received:%d\n", len);
                            if (load_recv_message.status == OK_WAIT_FOR_RESPONSE &&
                                (int) load_recv_message.length > 0) {
                                //printf("Message Received with length:%d\n", (int) load_recv_message.length);
                                // Calculate number of bytes in response package
                                int num_bytes = (int) load_recv_message.length;
                                char payloadRecv[num_bytes + 1];

                                // Receive the payload and print it out
                                if ((len = recv(client_socket, payloadRecv, num_bytes, 0)) > 0) {
                                    payloadRecv[num_bytes] = '\0';
                                    printf("%s\n", payloadRecv);

                                    string_curr_length = 0;

                                    for(i=0; i<nCols; i++){
                                        memset(col_values[i], '\0', col_val_capacity);
                                        strcpy(col_values[i], "load(");
                                        strcat(col_values[i], col_names[i]);
                                        strcat(col_values[i], ",");
                                    }
                                }
                                else{
                                    printf("Message payload is zero bytes");
                                }
                            }
                            else{
                               // printf("Length Received But Message Length:%d\n", (int) load_recv_message.length);
                            }
                        }
                        else {
                            if(payload != NULL)
                                free(payload);
                            if (len < 0) {
                                log_err("Failed to receive message.");
                            }
                            else {
                                log_info("Server closed connection\n");
                            }
                            exit(1);
                        }

                        if(payload != NULL)
                            free(payload);
                        
                    }
                    //cs165_log(stdout, "%s \n", col_values[0]);
                    //cs165_log(stdout, "len: %zu \n", string_curr_length);
                }
                if(string_curr_length > 0){
                    size_t payload_length = 0;
                    for(i=0; i<nCols; i++){
                        size_t len = strlen(col_values[i]);
                        int last_char = len - 1;
                        //col_values[i][last_char] = '\0';
                        if(i == nCols-1)
                        {
                            col_values[i][last_char] = ')';
                            col_values[i][last_char+1] = '\n';
                            col_values[i][last_char+2] = '\0';
                        }
                        else
                        {
                            col_values[i][last_char] = ')';
                            col_values[i][last_char+1] = ',';
                            col_values[i][last_char+2] = '\0';
                        }
                        payload_length += (len+2);
                    }
    
                    char *payload = NULL;
                    payload_length += (nCols*5);
                    payload = (char*) malloc(sizeof(char)* (payload_length));
                    memset(payload, '\0', (payload_length));

                    for(i=0; i<nCols; i++){
                        strcat(payload, col_values[i]);
                    }
                    load_send_message.length = payload_length;

                    //cs165_log(stdout, payload);
                    //printf("payload_length:%zu\n", payload_length);
                    if (send(client_socket, &(load_send_message), sizeof(message), 0) == -1) {
                        log_err("Failed to send message header.");
                        exit(1);
                    }

                    // Send the payload (query) to server
                    if (send(client_socket, payload, load_send_message.length, 0) == -1) {
                        log_err("Failed to send query payload.");
                        exit(1);
                    }
                    
                    // Always wait for server response (even if it is just an OK message)
                    if ((len = recv(client_socket, &(load_recv_message), sizeof(message), 0)) > 0) {
                        //printf("Length Received:%d\n", len);
                        if (load_recv_message.status == OK_WAIT_FOR_RESPONSE &&
                            (int) load_recv_message.length > 0) {
                            //printf("Message Received with length:%d\n", (int) load_recv_message.length);
                            // Calculate number of bytes in response package
                            int num_bytes = (int) load_recv_message.length;
                            char payloadRecv[num_bytes + 1];

                            // Receive the payload and print it out
                            if ((len = recv(client_socket, payloadRecv, num_bytes, 0)) > 0) {
                                payloadRecv[num_bytes] = '\0';
                                printf("%s\n", payloadRecv);

                                string_curr_length = 0;

                                for(i=0; i<nCols; i++){
                                    memset(col_values[i], '\0', col_val_capacity);
                                    strcpy(col_values[i], "load(");
                                    strcat(col_values[i], col_names[i]);
                                    strcat(col_values[i], ",");
                                }
                            }
                            else{
                                printf("Message payload is zero bytes");
                            }
                        }
                        else{
                            printf("Length Received But Message Length:%d\n", (int) load_recv_message.length);
                        }
                    }
                    else {
                        if(payload != NULL)
                            free(payload);
                        if (len < 0) {
                            log_err("Failed to receive message.");
                        }
                        else {
                            log_info("Server closed connection\n");
                        }
                        exit(1);
                    }

                    if(payload != NULL)
                        free(payload);
                }
            }
            for(i=0; i<nCols; i++){
                free(col_names[i]); free(col_values[i]);
            }
            free(col_names); free(col_values);
           
            //Form the queries

            
            fclose(fd);
        }
        else{
            cs165_log(stdout, "File could not be opened");
        }    
        free(tempToFree); 
    }
 
}

int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;
    int len = 0;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        cs165_log(stdout, "-- %s", output_str);
        // Only process input that is greater than 1 character.
        // Ignore things such as new lines.
        // Otherwise, convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {

            if(strncmp(send_message.payload, "load", 4) == 0){
                parse_load_query(send_message.payload, client_socket);

            }
            else{
                // Send the message_header, which tells server payload size
                if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                    log_err("Failed to send message header.");
                    exit(1);
                }

                // Send the payload (query) to server
                if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                    log_err("Failed to send query payload.");
                    exit(1);
                }
 
                // Always wait for server response (even if it is just an OK message)
                if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
                    if (recv_message.status == OK_WAIT_FOR_RESPONSE &&
                        (int) recv_message.length > 0) {
                        // Calculate number of bytes in response package
                        int num_bytes = (int) recv_message.length;
                        char payload[num_bytes + 1];

                        // Receive the payload and print it out
                        if ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
                            payload[num_bytes] = '\0';
                            printf("%s\n", payload);
                        }
                    }
                }
                else {
                    if (len < 0) {
                        log_err("Failed to receive message.");
                    }
                    else {
    		            log_info("Server closed connection\n");
    		        }
                    exit(1);
                }
            }
        }
    }
    log_info("End of Main Client Socket Closed\n");
    close(client_socket);
    return 0;
}
