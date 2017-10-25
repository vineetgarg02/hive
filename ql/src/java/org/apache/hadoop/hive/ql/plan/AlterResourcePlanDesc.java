package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Alter Resource plans", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterResourcePlanDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = -3514685833183437279L;

  private String rpName;
  private String newName;
  private Integer queryParallelism;
  private WMResourcePlanStatus status;
  private boolean validate;

  public AlterResourcePlanDesc() {}

  private AlterResourcePlanDesc(String rpName, String newName, Integer queryParallelism,
      WMResourcePlanStatus status, boolean validate) {
    this.rpName = rpName;
    this.newName = newName;
    this.queryParallelism = queryParallelism;
    this.status = status;
    this.validate = validate;
  }

  public static AlterResourcePlanDesc createChangeParallelism(String rpName,
      int queryParallelism) {
    return new AlterResourcePlanDesc(rpName, null, queryParallelism, null, false);
  }

  public static AlterResourcePlanDesc createChangeStatus(
      String rpName, WMResourcePlanStatus status) {
    return new AlterResourcePlanDesc(rpName, null, null, status, false);
  }

  public static AlterResourcePlanDesc createValidatePlan(String rpName) {
    return new AlterResourcePlanDesc(rpName, null, null, null, true);
  }

  public static AlterResourcePlanDesc createRenamePlan(String rpName, String newName) {
    return new AlterResourcePlanDesc(rpName, newName, null, null, false);
  }

  @Explain(displayName="resourcePlanName", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getRpName() {
    return rpName;
  }

  public void setRpName(String rpName) {
    this.rpName = rpName;
  }

  @Explain(displayName="newResourcePlanName", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getNewName() {
    return newName;
  }

  public void setNewName(String newName) {
    this.newName = newName;
  }

  @Explain(displayName="queryParallelism", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Integer getQueryParallelism() {
    return queryParallelism;
  }

  public void setQueryParallelism(Integer queryParallelism) {
    this.queryParallelism = queryParallelism;
  }

  @Explain(displayName="status", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMResourcePlanStatus getStatus() {
    return status;
  }

  public void setStatus(WMResourcePlanStatus status) {
    this.status = status;
  }

  @Explain(displayName="shouldValidate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean shouldValidate() {
    return validate;
  }

  public void setValidate(boolean validate) {
    this.validate = validate;
  }
}
