package com.itclj.service.impl;

import com.itclj.service.ReplicationTablecfsService;
import org.springframework.stereotype.Service;

@Service
public class ReplicationTablecfsServiceImpl implements ReplicationTablecfsService {
    @Override
    public boolean isTableTypeAll(String tableName) {
       return true;
    }
}
