package com.atguigu.es.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @BelongsProject: atguigu-project-new-221126java
 * @BelongsPackage: com.atguigu.es.entity
 * @Author: Hywel
 * @CreateTime: 2023-03-21  18:10
 * @Description: TODO
 * @Version: 1.0
 */
@Data
public class User implements Serializable {
    private Integer age;
    private String name;
    private String remark;
}
