package io.github.dreamlike;

import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;
import sun.nio.ch.FileChannelImpl;

import java.io.FileDescriptor;
import java.nio.channels.FileChannel;

class NativeHelper {

    private static final long CHANNEL_FD_OFFSET;

    private static final long FD_OFFSET;

    static final byte IORING_OP_READ = 22;
    static final byte IORING_OP_WRITE = 23;

    static {
        try {
            CHANNEL_FD_OFFSET = PlatformDependent.objectFieldOffset(FileChannelImpl.class.getDeclaredField("fd"));
            FD_OFFSET = PlatformDependent.objectFieldOffset(FileDescriptor.class.getDeclaredField("fd"));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static int getFd(FileChannel fileChannel) {
        FileDescriptor jdkFd = (FileDescriptor) PlatformDependent.getObject(fileChannel, CHANNEL_FD_OFFSET);
        if (jdkFd == null) {
            throw new IllegalArgumentException("FileChannel is closed");
        }

        return PlatformDependent.getInt(jdkFd, FD_OFFSET);
    }

}
