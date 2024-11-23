package io.github.dreamlike;

import io.netty.buffer.ByteBuf;
import io.netty.channel.EventLoop;
import io.netty.channel.IoEvent;
import io.netty.channel.IoEventLoop;
import io.netty.channel.IoRegistration;
import io.netty.channel.uring.IoUringIoEvent;
import io.netty.channel.uring.IoUringIoHandle;
import io.netty.channel.uring.IoUringIoOps;
import io.netty.channel.uring.IoUringIoRegistration;
import io.netty.util.collection.LongObjectHashMap;
import io.netty.util.concurrent.FutureListener;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.AsyncFileLock;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.file.impl.AsyncFileImpl;
import io.vertx.core.impl.Arguments;
import io.vertx.core.impl.buffer.VertxByteBufAllocator;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.PromiseInternal;
import io.vertx.core.internal.buffer.BufferInternal;
import io.vertx.core.streams.impl.InboundBuffer;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Objects;

public class IoUringAsyncFile implements AsyncFile {

    private final int nativeFd;

    private final FileChannel fileChannel;

    private final ContextInternal context;

    private final Vertx vertx;
    private final LongObjectHashMap<Promise<IoUringIoEvent>> processingTask = new LongObjectHashMap<>();
    final IoUringIoHandle nettyIoUringHandle = new IoUringIoHandle() {
        @Override
        public void handle(IoRegistration registration, IoEvent ioEvent) {
            IoUringIoEvent event = (IoUringIoEvent) ioEvent;
            short data = event.data();
            Promise<IoUringIoEvent> ioEventPromise = processingTask.get(data);
            if (ioEventPromise != null) {
                ioEventPromise.tryComplete(event);
            }
        }

        @Override
        public void close() {

        }
    };
    private long writePos;
    private long writesOutstanding;
    private boolean overflow;
    private Handler<Throwable> exceptionHandler;
    private Handler<Void> drainHandler;
    private int maxWrites = 128 * 1024;    // TODO - we should tune this for best performance
    private int lwm = maxWrites / 2;
    private int readBufferSize = AsyncFileImpl.DEFAULT_READ_BUFFER_SIZE;
    private InboundBuffer<Buffer> queue;
    private Handler<Buffer> handler;
    private Handler<Void> endHandler;
    private long readPos;
    private long readLength = Long.MAX_VALUE;
    private IoUringIoRegistration ioRegistration;
    private short opsId = Short.MIN_VALUE;

    private IoUringAsyncFile(Vertx vertx, Context context, String path, OpenOptions openOptions) throws IOException {
        this.vertx = vertx;
        this.context = (ContextInternal) context;
        this.fileChannel = toChannel(path, openOptions);
        this.nativeFd = NativeHelper.getFd(fileChannel);

        this.queue = new InboundBuffer<>(context, 0);
        queue.handler(buff -> {
            if (buff.length() > 0) {
                handleBuffer(buff);
            } else {
                handleEnd();
            }
        });
        queue.drainHandler(v -> {
            doRead();
        });
    }

    public static Future<IoUringAsyncFile> open(Vertx vertx, String path, OpenOptions openOptions) {
        Objects.requireNonNull(path);
        Objects.requireNonNull(openOptions);
        ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
        EventLoop eventLoop = context.nettyEventLoop();

        PromiseInternal<IoUringAsyncFile> promise = context.promise();

        if (!(eventLoop instanceof IoEventLoop) || ((IoEventLoop) eventLoop).isCompatible(IoUringIoHandle.class)) {
            promise.tryFail(new UnsupportedOperationException("current transport dont support IoUring"));
        } else {
            context.executeBlocking(() -> new IoUringAsyncFile(vertx, context, path, openOptions))
                    .onSuccess(file -> ((IoEventLoop) eventLoop)
                            .register(file.nettyIoUringHandle)
                            .addListener(((FutureListener<IoRegistration>) future -> {
                                if (future.isSuccess()) {
                                    file.ioRegistration = (IoUringIoRegistration) future.getNow();
                                    promise.complete(file);
                                } else {
                                    promise.tryFail(future.cause());
                                }
                            })));
        }
        return promise.future();
    }

    private FileChannel toChannel(String path, OpenOptions options) throws IOException {
        Path file = Paths.get(path);
        HashSet<OpenOption> opts = new HashSet<>();
        if (options.isRead()) opts.add(StandardOpenOption.READ);
        if (options.isWrite()) opts.add(StandardOpenOption.WRITE);
        if (options.isCreate()) opts.add(StandardOpenOption.CREATE);
        if (options.isCreateNew()) opts.add(StandardOpenOption.CREATE_NEW);
        if (options.isSync()) opts.add(StandardOpenOption.SYNC);
        if (options.isDsync()) opts.add(StandardOpenOption.DSYNC);
        if (options.isDeleteOnClose()) opts.add(StandardOpenOption.DELETE_ON_CLOSE);
        if (options.isSparse()) opts.add(StandardOpenOption.SPARSE);
        if (options.isTruncateExisting()) opts.add(StandardOpenOption.TRUNCATE_EXISTING);
        FileChannel channel = FileChannel.open(file, opts);
        if (options.isAppend()) writePos = channel.size();
        return channel;
    }

    private void handleBuffer(Buffer buff) {
        Handler<Buffer> handler;
        synchronized (this) {
            handler = this.handler;
        }
        if (handler != null) {
            handler.handle(buff);
        }
    }

    private void handleEnd() {
        Handler<Void> endHandler;
        synchronized (this) {
            handler = null;
            endHandler = this.endHandler;
        }
        if (endHandler != null) {
            endHandler.handle(null);
        }
    }

    private void check() {
        if (!fileChannel.isOpen()) {
            throw new IllegalStateException("File handle is closed");
        }
    }

    @Override
    public synchronized IoUringAsyncFile handler(Handler<Buffer> handler) {
        check();
        this.handler = handler;
        if (handler != null) {
            doRead();
        } else {
            queue.clear();
        }
        return this;
    }

    @Override
    public IoUringAsyncFile pause() {
        check();
        queue.pause();
        return this;
    }

    @Override
    public synchronized IoUringAsyncFile resume() {
        check();
        queue.resume();
        return this;
    }

    @Override
    public synchronized IoUringAsyncFile endHandler(Handler<Void> handler) {
        check();
        this.endHandler = handler;
        return this;
    }

    @Override
    public synchronized IoUringAsyncFile setWriteQueueMaxSize(int maxSize) {
        Arguments.require(maxSize >= 2, "maxSize must be >= 2");
        check();
        this.maxWrites = maxSize;
        this.lwm = maxWrites / 2;
        return this;
    }

    @Override
    public synchronized boolean writeQueueFull() {
        check();
        return overflow;
    }

    @Override
    public synchronized IoUringAsyncFile drainHandler(Handler<Void> handler) {
        check();
        this.drainHandler = handler;
        return this;
    }

    @Override
    public synchronized IoUringAsyncFile exceptionHandler(Handler<Throwable> handler) {
        check();
        this.exceptionHandler = handler;
        return this;
    }

    @Override
    public Future<Void> write(Buffer buffer) {
        return null;
    }

    @Override
    public Future<Void> end() {
        return null;
    }

    @Override
    public AsyncFile fetch(long l) {
        return null;
    }

    @Override
    public Future<Void> close() {
        return null;
    }

    @Override
    public Future<Void> write(Buffer buffer, long l) {
        return null;
    }

    @Override
    public Future<Buffer> read(Buffer buffer, int offset, long position, int length) {
        Promise<Buffer> promise = context.promise();
        Objects.requireNonNull(buffer, "buffer");
        Arguments.require(offset >= 0, "offset must be >= 0");
        Arguments.require(position >= 0, "position must be >= 0");
        Arguments.require(length >= 0, "length must be >= 0");
        check();
        read0(buffer, offset, length, position, promise);
        return promise.future();
    }

    @Override
    public Future<Void> flush() {
        return null;
    }

    @Override
    public AsyncFile setReadPos(long l) {
        return null;
    }

    @Override
    public AsyncFile setReadLength(long l) {
        return null;
    }

    @Override
    public long getReadLength() {
        return 0;
    }

    @Override
    public AsyncFile setWritePos(long l) {
        return null;
    }

    @Override
    public long getWritePos() {
        return 0;
    }

    @Override
    public AsyncFile setReadBufferSize(int i) {
        return null;
    }

    @Override
    public long sizeBlocking() {
        return 0;
    }

    @Override
    public Future<Long> size() {
        return null;
    }

    @Override
    public AsyncFileLock tryLock(long l, long l1, boolean b) {
        return null;
    }

    @Override
    public Future<AsyncFileLock> lock(long l, long l1, boolean b) {
        return null;
    }


    private void read0(Buffer buffer, int offset, int len, long position, Promise<Buffer> promise) {
        ByteBuf directBuffer = VertxByteBufAllocator.DEFAULT.directBuffer(len);
        short nextOpsId = nextOpsId();
        Promise<IoUringIoEvent> promiseInternal = Promise.promise();
        processingTask.put(nextOpsId, promiseInternal);

        Future<IoUringIoEvent> future = promiseInternal.future();

        future.onSuccess(ioEvent -> {
            try {
                if (ioEvent.res() < 0) {
                    promise.tryFail(new IOException("read failed: " + ioEvent.res()));
                } else {
                    ByteBuf byteBuf = directBuffer.writerIndex(ioEvent.res());

                    promise.tryComplete(buffer.setBuffer(offset, BufferInternal.buffer(byteBuf)));
                }
            } finally {
                directBuffer.release();
            }
        });

        long readId = ioRegistration.submit(new IoUringIoOps(
                NativeHelper.IORING_OP_READ,
                0,
                (short) 0,
                nativeFd,
                0,
                directBuffer.memoryAddress(),
                len,
                position,
                nextOpsId
        ));
    }

    private final short nextOpsId() {
        short nextId = opsId++;
        while (processingTask.containsKey(nextId)) {
            nextId = opsId++;
        }
        return nextId;
    }

}
