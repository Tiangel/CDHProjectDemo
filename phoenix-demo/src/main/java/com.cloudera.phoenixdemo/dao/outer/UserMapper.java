package com.cloudera.phoenixdemo.dao.outer;

import com.cloudera.phoenixdemo.entity.User;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * <p>
 * Mapper 接口
 * </p>
 *
 * @author zhangxin
 * @since 2019-07-08
 */

@Repository
public interface UserMapper  {


    List<User> getPermission(@Param("id") Integer id);
    User getOne(@Param("username") String username);
    User getInfo(@Param("username") String username);

}
