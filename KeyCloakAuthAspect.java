package ru.gkomega.router.aop.aspect;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import ru.gkomega.router.exception.UnauthorizedException;
import ru.gkomega.router.model.dto.auth_dto.UserDto;
import ru.gkomega.router.service.AuthorizationService;

import javax.servlet.http.HttpServletRequest;
import java.util.Objects;

@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class KeyCloakAuthAspect {

    private final AuthorizationService authorizationService;

    private static final String AUTH_HEADER = "Authorization";
    private static final String SESSION_HEADER = "Session";

    @Pointcut("@annotation(ru.gkomega.router.aop.annotation.KeyCloakAuth) && " +
            "args(project,.., httpServletRequest)")
    public void keyCloakAuth(String project, HttpServletRequest httpServletRequest) {
    }


    @Around(value = "keyCloakAuth(project, httpServletRequest)", argNames = "pjP, project, httpServletRequest")
    public Object trace(ProceedingJoinPoint pjP, String project, HttpServletRequest httpServletRequest) throws Throwable {
        val token = httpServletRequest.getHeader(AUTH_HEADER);
        val session = httpServletRequest.getHeader(SESSION_HEADER);
        log.info("{} : {}", token, session);
        if (check(token, session, project)) {
            return pjP.proceed();
        } else {
            throw new UnauthorizedException("user not found");
        }
    }

    private boolean check(String token, String session, String project) {
        if (Objects.nonNull(token)) {
            val userDto = authorizationService.newCheckToken(token, session, project);
            return Objects.nonNull(userDto.getSub());
        }
        return false;
    }

}
