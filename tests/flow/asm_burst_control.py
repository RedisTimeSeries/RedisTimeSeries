"""Separate-process loopback burst probe; no socket buffer setters."""
import json,multiprocessing as mp,socket,ssl,time,pathlib,tempfile,subprocess

def send(listener, tls, cert, key, total):
    try:
        conn,_=listener.accept();conn.settimeout(8)
        if tls:
            ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(cert,key)
            conn=ctx.wrap_socket(conn,server_side=True)
        with conn:
            data=b'x'*(1024*1024)
            for _ in range(total//len(data)):conn.sendall(data)
    finally:listener.close()

def trial(tls,read_size,pause,cert,key):
    total=16*1024*1024;listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(8)
    addr=listener.getsockname();proc=mp.get_context('fork').Process(target=send,args=(listener,tls,cert,key,total));proc.start()
    start=time.monotonic();n=0;changes=[];minimum=maximum=last=0;error=None;conn=None;receiver_port=None
    try:
        conn=socket.create_connection(addr,timeout=6)
        if tls:conn=ssl.create_default_context(cafile=cert).wrap_socket(conn,server_hostname='localhost')
        receiver_port=conn.getsockname()[1]
        last=minimum=maximum=conn.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF)
        # Let sender build a queue before active draining, without changing buffers.
        time.sleep(.025)
        while n<total:
            if time.monotonic()-start>7:raise TimeoutError('trial wall deadline')
            data=conn.recv(min(read_size,total-n))
            if not data:raise RuntimeError('early EOF')
            n+=len(data);current=conn.getsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF)
            minimum=min(minimum,current);maximum=max(maximum,current)
            if current!=last and len(changes)<40:changes.append([round(time.monotonic()-start,6),n,current])
            last=current
            if pause and n%(64*1024)<len(data):time.sleep(pause)
    except Exception as e:error=repr(e)
    finally:
        if conn:conn.close()
        listener.close();proc.join(1)
        if proc.is_alive():proc.terminate();proc.join(1)
    print(json.dumps(dict(kind="independent_burst", sender_port=addr[1], receiver_port=receiver_port, tls=tls,read_size=read_size,pause=pause,bytes=n,seconds=round(time.monotonic()-start,3),minimum=minimum,maximum=maximum,final=last,changes=changes,error=error,sender_exit=proc.exitcode)),flush=True)

