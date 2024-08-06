package io.unitycatalog.server.service;

import io.unitycatalog.server.model.Privilege;
import java.util.List;
import lombok.Getter;

@Getter
public class UpdateAuthorizationRequest {
  List<UpdateAuthorization> changes;
}

@Getter
class UpdateAuthorization {
  String principal;
  List<Privilege> add;
  List<Privilege> remove;
}
