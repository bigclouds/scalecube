package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.logging.LoggingHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TransportPipelineFactory implements PipelineFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransportPipelineFactory.class);

  private final Protocol protocol;
  private final Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings = new ConcurrentHashMap<>();

  // Shared handlers
  private final ExceptionCaughtChannelHandler exceptionHandler = new ExceptionCaughtChannelHandler();
  private final MessageToByteEncoder<Message> serializerHandler;
  private final MessageToMessageDecoder<ByteBuf> deserializerHandler;
  private final AcceptorHandshakeChannelHandler acceptorHandshakeHandler;
  private final AcceptorRegistratorChannelHandler acceptorRegistratorHandler;
  private final LoggingHandler loggingHandler;
  private final NetworkEmulatorChannelHandler networkEmulatorHandler;
  private final MessageReceiverChannelHandler messageHandler;

  /**
   * Creates new TransportPipelineFactory with concrete transport and protocol.
   */
  public TransportPipelineFactory(ITransportSpi transportSpi, Protocol protocol, boolean useNetworkEmulator) {
    checkArgument(transportSpi != null);
    checkArgument(protocol != null);
    this.protocol = protocol;

    // Init shared handlers
    this.serializerHandler = new SharableSerializerHandler(protocol.getMessageSerializer());
    this.deserializerHandler = new SharableDeserializerHandler(protocol.getMessageDeserializer());
    this.loggingHandler = transportSpi.getLogLevel() != null ? new LoggingHandler(transportSpi.getLogLevel()) : null;
    this.acceptorHandshakeHandler = new AcceptorHandshakeChannelHandler(transportSpi);
    this.acceptorRegistratorHandler = new AcceptorRegistratorChannelHandler(transportSpi);
    this.networkEmulatorHandler = useNetworkEmulator ? new NetworkEmulatorChannelHandler(networkSettings) : null;
    this.messageHandler = new MessageReceiverChannelHandler(transportSpi);
  }

  @Override
  public void setAcceptorPipeline(Channel channel, ITransportSpi transportSpi) {
    ChannelPipeline pipeline = channel.pipeline();
    addProtocolHandlers(pipeline);
    if (loggingHandler != null) {
      pipeline.addLast("loggingHandler", loggingHandler);
    }
    pipeline.addLast("acceptorRegistrator", acceptorRegistratorHandler);
    pipeline.addLast("handshakeHandler", acceptorHandshakeHandler);
    pipeline.addLast("exceptionHandler", exceptionHandler);
  }

  @Override
  public void setConnectorPipeline(Channel channel, ITransportSpi transportSpi) {
    ChannelPipeline pipeline = channel.pipeline();
    addProtocolHandlers(pipeline);
    if (loggingHandler != null) {
      pipeline.addLast("loggingHandler", loggingHandler);
    }
    pipeline.addLast("handshakeHandler", new ConnectorHandshakeChannelHandler(transportSpi));
    pipeline.addLast("exceptionHandler", exceptionHandler);
  }

  @Override
  public void resetDueHandshake(Channel channel, ITransportSpi transportSpi) {
    ChannelPipeline pipeline = channel.pipeline();
    if (networkEmulatorHandler != null) {
      pipeline.addBefore("handshakeHandler", "networkEmulator", networkEmulatorHandler);
    }
    pipeline.remove("handshakeHandler");
    pipeline.addBefore(transportSpi.getEventExecutor(), "exceptionHandler", "messageReceiver", messageHandler);
  }

  private void addProtocolHandlers(ChannelPipeline pipeline) {
    pipeline.addLast("frameDecoder", protocol.getFrameHandlerFactory().newFrameDecoder());
    pipeline.addLast("deserializer", deserializerHandler);
    pipeline.addLast("frameEncoder", protocol.getFrameHandlerFactory().newFrameEncoder());
    pipeline.addLast("serializer", serializerHandler);
  }

  public void setNetworkSettings(TransportEndpoint endpoint, int lostPercent, int mean) {
    networkSettings.put(endpoint, new NetworkEmulatorSettings(lostPercent, mean));
  }

  public void blockMessagesTo(TransportEndpoint destination) {
    networkSettings.put(destination, new NetworkEmulatorSettings(100, 0));
    LOGGER.debug("Set BLOCK messages to {}", destination);
  }

  public void unblockMessagesTo(TransportEndpoint destination) {
    networkSettings.put(destination, new NetworkEmulatorSettings(0, 0));
    LOGGER.debug("Set UNBLOCK messages to {}", destination);
  }

  public void unblockAll() {
    networkSettings.clear();
    LOGGER.debug("Set UNBLOCK ALL messages");
  }

}
