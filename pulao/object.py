class Base:
    def __repr__(self):
        return f"{self.__class__.__name__}({self.__dict__})"

    def _repr_html_(self):
        self.__repr__()

    def to_dict(self, include_private=False):
        def convert(value):
            # 如果 value 有自己的 to_dict 方法，则递归调用
            if hasattr(value, "to_dict") and callable(value.to_dict):
                return value.to_dict(include_private=include_private)

            # 如果是列表，则递归每个元素
            if isinstance(value, list):
                return [convert(v) for v in value]

            # 如果是字典，则递归每个值
            if isinstance(value, dict):
                return {k: convert(v) for k, v in value.items()}

            # 其他类型直接返回
            return value

        # 选择属性来源
        items = (
            self.__dict__.items()
            if include_private
            else {
                k: v for k, v in self.__dict__.items() if not k.startswith("_")
            }.items()
        )

        # 递归转换所有属性
        return {k: convert(v) for k, v in items}
