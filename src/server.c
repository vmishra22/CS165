/** server.c
 * CS165 Fall 2015
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The server should allow for multiple concurrent connections from clients.
 * Each client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024

bool shutdown_requested = false;
/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
 void free_client_context(ClientContext* client_context){
    if(client_context != NULL){
        int i=0;
        for(i=0; i<client_context->chandles_in_use; i++){
            GeneralizedColumnHandle* pGenHandle = &(client_context->chandle_table[i]);
            Result* pResult = pGenHandle->generalized_column.column_pointer.result;
            if(pResult != NULL)
            {
                float* pPayloadFloat = NULL;
                int* pPayloadInt = NULL;
                void* payLoad = pResult->payload;
                if(pResult->data_type == INT)
                {
                    pPayloadInt = (int*)payLoad;
                    if(pPayloadInt != NULL)
                        free(pPayloadInt);
                }else{
                    pPayloadFloat = (float*)payLoad;
                    if(pPayloadFloat != NULL)
                        free(pPayloadFloat);
                }
                free(pResult);
            }
        }
        free(client_context->chandle_table);
        free(client_context);
    }
 }

 ClientContext* allocate_client_context(){
    ClientContext* client_context = (ClientContext*)malloc(sizeof(ClientContext));

    memset(client_context, 0, sizeof(ClientContext));
    client_context->chandle_slots = 10;
    client_context->chandles_in_use = 0;
    GeneralizedColumnHandle* pColumnHandle = 
        (GeneralizedColumnHandle*) malloc(sizeof(GeneralizedColumnHandle)*(client_context->chandle_slots));
    memset(pColumnHandle, 0, sizeof(GeneralizedColumnHandle)*(client_context->chandle_slots));
    client_context->chandle_table = pColumnHandle;

    return client_context;
 }

void handle_client(int client_socket) {
    int done = 0;
    int length = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = NULL; //TO DO: There can be multiple clients connected to the server
    send_message.status = OK_WAIT_FOR_RESPONSE;

    client_context = allocate_client_context();

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response of request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            free_client_context(client_context);
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';

            // 1. Parse command
            DbOperator* query = NULL;
            query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            // 2. Handle request
            char* payload_to_client = (char*)malloc(MAX_RESPONSE_SIZE);
            memset(payload_to_client, '\0', MAX_RESPONSE_SIZE);

            //Execute db operator
            execute_DbOperator(query, &payload_to_client);

            char retMessage[MAX_RESPONSE_SIZE];
            strcpy(retMessage, payload_to_client);
            free(payload_to_client);

            //cs165_log(stdout, retMessage);
            if(send_message.status == SHUTDOWN_REQUESTED)
                shutdown_requested = true;

            send_message.length = strlen(retMessage);
            send_message.payload = NULL;

            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            // 4. Send response of request            
            if (send(client_socket, retMessage, send_message.length, 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }
        }
    } while (!done);

    free_client_context(client_context);

    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    // if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
    //     log_err("L%d: Failed to accept a new connection.\n", __LINE__);
    //     exit(1);
    // }

    db_startup();

  //  handle_client(client_socket);

    while(!shutdown_requested){
        if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
            log_err("L%d: Failed to accept a new connection.\n", __LINE__);
            exit(1);
        }
        handle_client(client_socket);
    };

    

    return 0;
}

