package io.unitycatalog.server.service;

import io.unitycatalog.server.model.Privilege;
import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
class PrivilegeAssignment {
  String principal;
  List<Privilege> privileges;
}
