package io.unitycatalog.server.service;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class UpdateAuthorizationResponse {
  List<PrivilegeAssignment> privilege_assignments;
}
