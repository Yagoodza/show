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

@Slf4j
@Aspect
@Component
@RequiredArgsConstructor
public class KeyCloakAuthAspect {

    private final AuthorizationService authorizationService;

    private static final String AUTH_HEADER = "Authorization";
    private static final String SESSION_HEADER = "Session";


    // инфа тута
    // https://habr.com/ru/post/428548/
    // https://jstobigdata.com/spring/pointcut-expressions-in-spring-aop/

    @Pointcut("@annotation(ru.gkomega.router.aop.annotation.KeyCloakAuth) && " +
            "args(project,.., httpServletRequest)")
    public void keyCloakAuth(String project, HttpServletRequest httpServletRequest) {
    }


    @Around(value = "keyCloakAuth(project, httpServletRequest)", argNames = "pjP, project, httpServletRequest")
    public Object trace(ProceedingJoinPoint pjP, String project, HttpServletRequest httpServletRequest) throws Throwable {
        val token = httpServletRequest.getHeader(AUTH_HEADER);
        val session = httpServletRequest.getHeader(SESSION_HEADER);
        log.info("{} : {}", token, session);
        httpServletRequest.setAttribute("user", getUser(token, session, project));
        return pjP.proceed();
    }

    private UserDto getUser(String token, String session, String project) {
            return authorizationService.info(project, token);
    }

}
