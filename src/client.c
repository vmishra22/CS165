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
        if(fd != NULL){
            char buf[1024]; //TO CHECK: If buffer size if enough?
            char table_Name[256];
            char query_insert[1024];
            memset(buf, '\0', 1024);
            memset(table_Name, '\0', 256);
            memset(query_insert, '\0', 1024);
            if(fgets(buf, 1024, fd))
            {
                char* tmp, *tmpToFree;
                tmp = tmpToFree = strdup(buf);
                char* tkdb = strsep(&tmp, ".");
                char* tktable = strsep(&tmp, ".");
                strcat(table_Name, tkdb);
                strcat(table_Name, ".");
                strcat(table_Name, tktable);
                strcat(query_insert, "relational_insert(");
                strcat(query_insert, table_Name);
                strcat(query_insert, ",");
                free(tmpToFree);
            }
            memset(buf, '\0', 1024);
            char newQueryInsert[1024];
            while (fgets(buf, 512, fd))
            {
                char *tempStr1, *tempStr1ToFree;
                tempStr1 = tempStr1ToFree = strdup(buf);
                char* tmptk = strsep(&tempStr1, "\n");

                memset(newQueryInsert, '\0', 1024);
                strcat(newQueryInsert, query_insert);
                strcat(newQueryInsert, tmptk);
                strcat(newQueryInsert, ")\n");
                free(tempStr1ToFree);

                char payload[DEFAULT_STDIN_BUFFER_SIZE];
                strcpy(payload, newQueryInsert);
                load_send_message.length = strlen(payload);

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

                //sleep(2);
                
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
                    if (len < 0) {
                        log_err("Failed to receive message.");
                    }
                    else {
                        log_info("Server closed connection\n");
                    }
                    exit(1);
                }

                
            }
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
