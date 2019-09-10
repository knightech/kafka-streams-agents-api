package net.knightech.agent.domain.beans;

import lombok.Builder;
import lombok.Data;
import net.knightech.agent.Agent;

@Builder
@Data
public class AgentBean {

  private String id;
  private String firstName;
  private String lastName;
  private String email;

  public static AgentBean toBean(final Agent agent) {
    return AgentBean.builder().id(agent.getId()).firstName(agent.getFirstName()).lastName(agent.getLastName()).email(agent.getEmail()).build();
  }

  public static Agent fromBean(final AgentBean agentBean) {
    return new Agent(agentBean.getId(), agentBean.getFirstName(), agentBean.getLastName(), agentBean.getEmail());
  }
}