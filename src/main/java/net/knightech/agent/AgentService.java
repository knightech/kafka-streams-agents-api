package net.knightech.agent;

public interface AgentService {

  void start(String bootstrapServers, String stateDir);

  void stop();
}