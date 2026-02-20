-- Support_logs table creation -- 
CREATE TABLE  public.support_logs (
    timestamp       TIMESTAMP,
    log_level       VARCHAR(20),
    component       VARCHAR(100),
    ticket_id       VARCHAR(50),
    session_id      VARCHAR(50),
    ip              VARCHAR(45),
    response_time   BIGINT,
    cpu             DOUBLE PRECISION,
    event_type      VARCHAR(50),
    error           BOOLEAN,
    user_agent      VARCHAR(300),
    message         VARCHAR(1000),
    debug           VARCHAR(1000)
);

-- Data insertion from S3 Bucket of support-logs --
COPY public.support_logs
FROM 's3://careplus-dataplus-store/support-logs/processed-data/'
IAM_ROLE 'arn:aws:iam::666777:role/service-role/AmazonRedshift-CommandsAccessRole-123123'
FORMAT AS PARQUET
REGION 'eu-central-1';


-- Support_tickets table creation -- 
CREATE TABLE public.support_tickets (
    ticket_id VARCHAR(50),
    created_at TIMESTAMP,
    resolved_at TIMESTAMP,
    agent VARCHAR(100),
    priority VARCHAR(20),
    num_interactions BIGINT,
    issue_category VARCHAR(100),
    channel VARCHAR(50),
    status VARCHAR(20)
);

-- Data insertion from S3 Bucket of support-logs --
COPY public.support_tickets
FROM 's3://careplus-dataplus-store/support-tickets/processed-data/'
IAM_ROLE 'arn:aws:iam::666777:role/service-role/AmazonRedshift-CommandsAccessRole-123123'
FORMAT AS PARQUET
REGION 'eu-central-1';
