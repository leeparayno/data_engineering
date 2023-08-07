import threading, os, time, paramiko

#you could make the number of threads relative to file size
NUM_THREADS = 4
MAX_RETRIES = 10
ftp_server = 'sftp.vertexinc.com'
port = 22
sftp_file = '/disney/disney/Hulu.zip'
local_file = "/Users/Lee.Parayno/Downloads/Hulu.zip"
ssh_conn = sftp_client = None
username = 'disney'
password = 'fqc#JFt9U26)'

def make_filepart_path(file_path, part_number):
    """creates filepart path from filepath"""
    return "%s.filepart.%s" % (file_path, part_number+1)

def write_chunks(chunks, tnum, local_file_part, username, password, ftp_server, max_retries):
    ssh_conn = sftp_client = None
    for retry in range(max_retries):
        try:
            ssh_conn = paramiko.Transport((ftp_server, port))
            ssh_conn.connect(username=username, password=password)
            sftp_client = paramiko.SFTPClient.from_transport(ssh_conn)
            with sftp_client.open(sftp_file, "rb") as infile:
                with open(local_file_part, "wb") as outfile:
                    for chunk in infile.readv(chunks):
                        outfile.write(chunk)
            break
        except (EOFError, paramiko.ssh_exception.SSHException, OSError) as x:
            retry += 1
            print("%s %s Thread %s - > retrying %s..." % (type(x), x, tnum, retry))
            time.sleep(abs(retry) * 10)
        finally:
            if hasattr(sftp_client, "close") and callable(sftp_client.close):
                sftp_client.close()
            if hasattr(ssh_conn, "close") and callable(ssh_conn.close):
                ssh_conn.close()



start_time = time.time()

for retry in range(MAX_RETRIES):
    try:
        ssh_conn = paramiko.Transport((ftp_server, port))
        ssh_conn.connect(username=username, password=password)
        sftp_client = paramiko.SFTPClient.from_transport(ssh_conn)
        # connect to get the file's size in order to calculate chunks
        filesize = sftp_client.stat(sftp_file).st_size
        sftp_client.close()
        ssh_conn.close()
        chunksize = pow(4, 11) # 4 MB
        chunks = [(offset, chunksize) for offset in range(0, filesize, chunksize)]
        thread_chunk_size = (len(chunks) // NUM_THREADS) + 1
        # break the chunks into sub lists to hand off to threads
        thread_chunks = [chunks[i:i+thread_chunk_size] for i in range(0, len(chunks) - 1, thread_chunk_size)]
        threads = []
        fileparts = []
        for thread_num in range(len(thread_chunks)):
            local_file_part = make_filepart_path(local_file, thread_num) 
            args = (thread_chunks[thread_num], thread_num, local_file_part, username, password, ftp_server, MAX_RETRIES)
            threads.append(threading.Thread(target=write_chunks, args=args))
            fileparts.append(local_file_part)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        # join file parts into one file, remove fileparts
        with open(local_file, "wb") as outfile:
            for filepart in fileparts:
                with open(filepart, "rb") as infile:
                    outfile.write(infile.read())
                os.remove(filepart)
        break
    except (EOFError, paramiko.ssh_exception.SSHException, OSError) as x:
        retry += 1
        print("%s %s - > retrying %s..." % (type(x), x, retry))
        time.sleep(abs(retry) * 10)
    finally:
       if hasattr(sftp_client, "close") and callable(sftp_client.close):
           sftp_client.close()
       if hasattr(ssh_conn, "close") and callable(ssh_conn.close):
           ssh_conn.close()


print("Loading File %s Took %d seconds " % (sftp_file, time.time() - start_time))