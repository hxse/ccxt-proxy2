/* 只连受控端点；用于证明 32 位 Wine WinSock 经 HTTP CONNECT。 */
#include <winsock2.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    WSADATA data;
    if (argc != 3 || WSAStartup(MAKEWORD(2, 2), &data)) return 2;
    SOCKET fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = inet_addr(argv[1]);
    address.sin_port = htons((unsigned short)atoi(argv[2]));
    if (connect(fd, (struct sockaddr *)&address, sizeof(address))) {
        closesocket(fd); WSACleanup(); return 3;
    }
    const char *message = "cfb-native-connect";
    char reply[64] = {0};
    int sent = send(fd, message, (int)strlen(message), 0);
    int received = recv(fd, reply, sizeof(reply), 0);
    closesocket(fd); WSACleanup();
    if (sent != (int)strlen(message) || received != sent || memcmp(reply, message, sent)) return 4;
    puts("PASS: native TCP roundtrip"); return 0;
}
