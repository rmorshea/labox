class X:
    def __init_subclass__(cls) -> None:
        print(Y)


class Y(X): ...
