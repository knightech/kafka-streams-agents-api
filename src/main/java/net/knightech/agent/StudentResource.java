package net.knightech.agent;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class StudentResource {


    @GetMapping("/students")
    public List<Student> retrieveAllStudents() {

        return new ArrayList<>(Arrays.asList(Student.builder().name("bob").build(), Student.builder().name("madge").build(), Student.builder().name("fisher").build()));
    }


}