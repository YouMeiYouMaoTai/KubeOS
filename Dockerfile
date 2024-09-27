# 基于openEuler的基础镜像
FROM openeuler/openeuler:22.03-lts-sp3

# 将operator文件拷贝到容器内的根目录，设置权限为500，并指定运行的用户
COPY --chown=6552:6552 ./bin/rust/release/operator /operator
RUN chmod 500 /operator

# 设置容器启动时运行的命令
ENTRYPOINT ["/operator"]

# # 基于openEuler的基础镜像
# FROM openeuler/openeuler:22.03-lts-sp3

# # 将proxy文件拷贝到容器内的根目录，设置权限为500
# COPY ./bin/rust/release/proxy /proxy
# RUN chmod 500 /proxy && chown root:root /proxy

# # 设置容器启动时运行的命令
# ENTRYPOINT ["/proxy"]
