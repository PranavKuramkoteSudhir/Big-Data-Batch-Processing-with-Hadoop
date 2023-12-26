
import socket
import news_api
import time
import threading


def send_data(conn):
    try:
        while True:
            data = news_api.run_news_api()
            serialized_data = data.to_json(orient='records').encode('utf-8')
            conn.send(serialized_data + b'\n')
            time.sleep(20)
    except Exception as e:
        print(f"Error in sending data: {e}")
    finally:
        conn.close()
        print("Connection closed")


def start_server(host='spark-master', port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f"Listening for connections:\nHost: {host}\nPort: {port}")

        while True:
            try:
                conn, addr = s.accept()
                print(f"Connection established:\nAddress: {addr}")

                client_thread = threading.Thread(target=send_data, args=(conn,))
                client_thread.start()

            except KeyboardInterrupt:
                print("Server terminated by user.")
                break
            except Exception as e:
                print(f"Error in accepting connection: {e}")


if __name__ == "__main__":
    start_server()
