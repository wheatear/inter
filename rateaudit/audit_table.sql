-- Create table
create table ZG.rate_audit
(
  audit_code VARCHAR2(32),
  item_code NUMBER(8),
audit_date date
);
-- Add comments to the columns
comment on column ZG.rate_audit.audit_code   is '稽核结果： error_rate          税率错误
duplicate_item      科目重复
empty_rate          空税率
conflict_item       科目冲突
consistent          稽核一致';
comment on column ZG.rate_audit.item_code    is '项目代码，包括科目或税率代码';
comment on column ZG.rate_audit.audit_date    is '稽核时间';
