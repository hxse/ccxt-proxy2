FROM docker.io/library/python:3.13-slim-bookworm@sha256:3e2de9c40ca4e3d73240059f9d48baff27908f10293e985a2f382a0378e6df4a AS fixtures
RUN apt-get update && apt-get install -y --no-install-recommends gcc-mingw-w64-i686-posix && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY containers/cfb/native ./native
COPY Test/cfb_native ./tests
RUN mkdir /fixtures && for name in browser settlement gui_readiness control_scopes; do \
      case "$name" in \
        control_scopes) parts='native/gui.c native/grid.c' ;; \
        gui_readiness) parts='native/startup.c native/browser.c native/readiness.c' ;; \
        *) parts='native/startup.c native/browser.c' ;; \
      esac; \
      i686-w64-mingw32-gcc -Wall -Wextra -Werror -Wno-unused-parameter -O2 -static-libgcc -municode \
        -I native "tests/native_$name.c" native/common.c $parts -o "/fixtures/$name.exe" -lole32 -loleaut32 -luuid || exit 1; \
    done \
    && i686-w64-mingw32-gcc -Wall -Wextra -Werror -O2 -static-libgcc tests/native_connect.c -o /fixtures/connect.exe -lws2_32

FROM localhost/ccxt-proxy2-cfb:verification-runtime AS verification
LABEL io.ccxt-proxy2.kind="cfb-verification"
RUN apt-get update && apt-get install -y --no-install-recommends x11-xserver-utils && rm -rf /var/lib/apt/lists/*
COPY containers/cfb/pyproject.toml containers/cfb/uv.lock /tmp/test-dependencies/
RUN pip install --no-cache-dir uv==0.12.17 && cd /tmp/test-dependencies && UV_PROJECT_ENVIRONMENT=/app/.venv uv sync --locked
COPY --from=fixtures /fixtures /fixtures
COPY Test/cfb_native /app/Test/cfb_native
CMD ["python", "-m", "pytest", "-v", "-ra", "Test/cfb_native"]
